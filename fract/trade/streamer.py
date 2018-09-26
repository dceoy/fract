#!/usr/bin/env python

import logging
import json
import os
import signal
import sqlite3
import oandapy
import redis
from ..cli.util import read_config_yml


class StreamDriver(oandapy.Streamer):
    def __init__(self, config_dict, target='rate', instruments=None,
                 ignore_heartbeat=True, redis_host='127.0.0.1',
                 redis_port=6379, redis_db=0, redis_max_llen=None,
                 sqlite_path=None, quiet=False):
        self.logger = logging.getLogger(__name__)
        super().__init__(
            environment=config_dict['oanda']['environment'],
            access_token=config_dict['oanda']['access_token']
        )
        self.account_id = config_dict['oanda']['account_id'],
        self.target = target
        self.instruments = (
            instruments if instruments else config_dict['instruments']
        )
        self.ignore_heartbeat = ignore_heartbeat
        self.quiet = quiet
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.target]
        if redis_host:
            self.logger.info('Set a streamer with Redis')
            self.redis_pool = redis.ConnectionPool(
                host=redis_host, port=redis_port, db=redis_db
            )
            self.redis = redis.StrictRedis(connection_pool=self.redis_pool)
            self.redis.flushdb()
            self.redis_max_llen = redis_max_llen
        else:
            self.redis_pool = None
            self.redis = None
            self.redis_max_llen = None
        if sqlite_path:
            self.logger.info('Set a streamer with SQLite')
            if os.path.isfile(sqlite_path):
                self.sqlite = sqlite3.connect(sqlite_path)
            else:
                schema_sql_path = os.path.join(
                    os.path.dirname(__file__), '../static/create_tables.sql'
                )
                with open(schema_sql_path, 'r') as f:
                    sql = f.read()
                self.sqlite = sqlite3.connect(sqlite_path)
                self.sqlite.executescript(sql)
        else:
            self.sqlite = None

    def on_success(self, data):
        data_json_str = json.dumps(data)
        if not self.quiet:
            print(data_json_str)
        if 'disconnect' in data:
            self.logger.warning('Streaming disconnected: {}'.format(data))
            self.disconnect()
            if self.redis:
                self.redis.connection_pool.disconnect()
            if self.sqlite:
                self.sqlite.close()
        elif self.key in data:
            self.logger.debug(data)
            if self.redis:
                instrument = data[self.key]['instrument']
                self.redis.rpush(instrument, data_json_str)
                if self.redis_max_llen:
                    llen = self.redis.llen(instrument)
                    self.logger.debug('llen: {}'.format(llen))
                    if llen > self.redis_max_llen:
                        self.redis.lpop(instrument)
            if self.sqlite:
                c = self.sqlite.cursor()
                if 'tick' in data:
                    c.execute(
                        'INSERT INTO tick VALUES (?,?,?,?)',
                        [
                            data['tick']['instrument'], data['tick']['time'],
                            data['tick']['bid'], data['tick']['ask']
                        ]
                    )
                    self.sqlite.commit()
                elif 'transaction' in data:
                    c.execute(
                        'INSERT INTO event VALUES (?,?,?)',
                        [
                            data['transaction']['instrument'],
                            data['transaction']['time'],
                            json.dumps(data['transaction'])
                        ]
                    )
                    self.sqlite.commit()
                else:
                    self.logger.warning(data)
        else:
            self.logger.debug('Save skipped: {}'.format(data))

    def on_error(self, data):
        self.logger.error(data)
        self.disconnect()
        if self.redis:
            self.redis.connection_pool.disconnect()
        if self.sqlite:
            self.sqlite.close()

    def invoke(self):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.target == 'rate':
            self.logger.info('Start to stream market prices')
            self.rates(
                account_id=self.account_id,
                ignore_heartbeat=self.ignore_heartbeat,
                instruments=','.join(self.instruments)
            )
        elif self.target == 'event':
            self.logger.info('Start to stream events for the account')
            self.events(
                account_id=self.account_id,
                ignore_heartbeat=self.ignore_heartbeat
            )


def invoke_streamer(config_yml, target='rate', instruments=None,
                    sqlite_path=None, redis_host=None, redis_port=None,
                    redis_db=None, redis_max_llen=None, quiet=False):
    logger = logging.getLogger(__name__)
    logger.info('Streaming')
    cf = read_config_yml(path=config_yml)
    rd = cf['redis'] if 'redis' in cf else {}
    streamer = StreamDriver(
        config_dict=cf, target=target, instruments=instruments,
        redis_host=(redis_host or rd.get('host')),
        redis_port=(int(redis_port) if redis_port else rd.get('port')),
        redis_db=(int(redis_db) if redis_db else rd.get('db')),
        redis_max_llen=(
            int(redis_max_llen) if redis_max_llen else rd.get('max_llen')
        ),
        sqlite_path=sqlite_path, quiet=quiet
    )
    streamer.invoke()