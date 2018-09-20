#!/usr/bin/env python

import logging
import json
import os
import sqlite3
import oandapy
import redis


class StreamDriver(oandapy.Streamer):
    def __init__(self, environment, access_token, account_id, target='rate',
                 instruments=None, ignore_heartbeat=True, use_redis=False,
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 redis_max_llen=None, sqlite_path=None, quiet=False, **kwargs):
        self.logger = logging.getLogger(__name__)
        super().__init__(
            environment=environment, access_token=access_token, **kwargs
        )
        self.account_id = account_id
        self.target = target
        self.instruments = instruments
        self.ignore_heartbeat = ignore_heartbeat
        self.quiet = quiet
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.target]
        if use_redis:
            self.logger.info('Set a streamer with Redis')
            self.redis_pool = redis.ConnectionPool(
                host=redis_host, port=redis_port, db=redis_db
            )
            self.redis = redis.StrictRedis(connection_pool=self.redis_pool)
            self.redis.flushdb()
            self.redis_max_llen = redis_max_llen
        else:
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
        if self.quiet:
            self.logger.debug(data)
        else:
            print(data)
        if 'disconnect' in data:
            self.disconnect()
            if self.redis:
                self.redis.connection_pool.disconnect()
            if self.sqlite:
                self.sqlite.close()
        else:
            if self.redis:
                instrument = data[self.key]['instrument']
                self.redis.rpush(instrument, json.dumps(data))
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

    def on_error(self, data):
        self.logger.error(data)
        self.disconnect()
        if self.redis:
            self.redis.connection_pool.disconnect()
        if self.sqlite:
            self.sqlite.close()

    def run(self, **kwargs):
        if self.target == 'rate':
            self.logger.info('Start to stream market prices')
            self.rates(
                account_id=self.account_id,
                ignore_heartbeat=self.ignore_heartbeat,
                instruments=','.join(self.instruments), **kwargs
            )
        elif self.target == 'event':
            self.logger.info('Start to stream events for the account')
            self.events(
                account_id=self.account_id,
                ignore_heartbeat=self.ignore_heartbeat, **kwargs
            )
