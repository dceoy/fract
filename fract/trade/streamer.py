#!/usr/bin/env python

import logging
import json
import os
import signal
import sqlite3
import oandapy
import redis
from ..cli.util import FractError


class RateCacheStreamer(oandapy.Streamer):
    def __init__(self, environment, access_token, account_id, instruments,
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 redis_maxl=1000, ignore_heartbeat=True):
        super().__init__(environment=environment, access_token=access_token)
        self.logger = logging.getLogger(__name__)
        self.account_id = account_id
        self.instruments = instruments
        self.ignore_heartbeat = ignore_heartbeat
        self.logger.info('Set a streamer with Redis')
        self.redis = redis.StrictRedis(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.redis_maxl = int(redis_maxl)
        self.redis.flushdb()

    def on_success(self, data):
        self.logger.debug(data)
        if 'tick' in data and 'instrument' in data['tick']:
            instrument = data['tick']['instrument']
            self.redis.rpush(instrument, json.dumps(data))
            if self.redis.llen(instrument) > self.redis_maxl:
                self.redis.lpop(instrument)
        else:
            raise FractError('data[\'tick\'][\'instrument\'] not found')
        if 'disconnect' in data:
            self.disconnect()
            self.redis.connection_pool.disconnect()

    def on_error(self, data):
        self.logger.error(data)
        self.disconnect()
        self.redis.connection_pool.disconnect()

    def invoke(self, **kwargs):
        self.logger.info('Start to stream market prices')
        self.rates(
            account_id=self.account_id, instruments=','.join(self.instruments),
            ignore_heartbeat=self.ignore_heartbeat, **kwargs
        )


class StorageStreamer(oandapy.Streamer):
    def __init__(self, target, sqlite_path=None, use_redis=False,
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 redis_maxl=1000, **kwargs):
        super().__init__(**kwargs)
        self.target = target
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.target]
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
        if use_redis:
            self.logger.info('Set a streamer with Redis')
            self.redis = redis.StrictRedis(
                host=redis_host, port=int(redis_port), db=int(redis_db)
            )
            self.redis.flushdb()
        else:
            self.redis = None
        self.redis_maxl = int(redis_maxl)

    def on_success(self, data):
        print(data)
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
        if self.redis:
            instrument = data[self.key]['instrument']
            self.redis.rpush(instrument, json.dumps(data))
            if self.redis.llen(instrument) > self.redis_maxl:
                self.redis.lpop(instrument)
        if 'disconnect' in data:
            self.disconnect()
            if self.sqlite:
                self.sqlite.close()
            if self.redis:
                self.redis.connection_pool.disconnect()

    def on_error(self, data):
        self.logger.error(data)
        self.disconnect()
        if self.sqlite:
            self.sqlite.close()
        if self.redis:
            self.redis.connection_pool.disconnect()

    def invoke(self, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.target == 'rate':
            self.logger.info('Start to stream market prices')
            self.rates(**kwargs)
        elif self.target == 'event':
            self.logger.info('Start to stream authorized account\'s events')
            self.events(**kwargs)
