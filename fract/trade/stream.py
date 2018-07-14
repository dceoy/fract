#!/usr/bin/env python

import logging
import json
import os
import signal
import sqlite3
import oandapy
import redis


class BaseStreamer(oandapy.Streamer):
    def __init__(self, target, logger=None, **kwargs):
        super().__init__(**kwargs)
        self.target = target
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.target]
        self.logger = logging.getLogger(__name__)

    def on_success(self, data):
        print(data)
        if 'disconnect' in data:
            self.disconnect()

    def on_error(self, data):
        self.logger.error(data)
        self.disconnect()

    def invoke(self, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.target == 'rate':
            self.logger.debug('Stream market prices')
            self.rates(**kwargs)
        elif self.target == 'event':
            self.logger.debug('Stream account events')
            self.events(**kwargs)


class StreamDriver(oandapy.Streamer):
    def __init__(self, target, sqlite_path=None, redis_host=None,
                 redis_port=6379, redis_db=0, redis_maxl=1000, **kwargs):
        super().__init__(**kwargs)
        self.target = target
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.target]
        if sqlite_path:
            self.logger.debug('Set a streamer with SQLite')
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
        if redis_host:
            self.logger.debug('Set a streamer with Redis')
            self.redis = redis.StrictRedis(
                host=redis_host, port=int(redis_port), db=int(redis_db)
            )
            self.redis_maxl = int(redis_maxl)
            self.redis.flushdb()
        else:
            self.redis = None

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

    def fire(self, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.target == 'rate':
            self.logger.debug('Start to stream market prices')
            self.rates(**kwargs)
        elif self.target == 'event':
            self.logger.debug('Start to stream authorized account\'s events')
            self.events(**kwargs)


def invoke_stream(target, instruments, config, sqlite_path=None,
                  redis_host=None, redis_port=6379, redis_db=0,
                  redis_maxl=1000):
    logger = logging.getLogger(__name__)
    logger.info('Streaming')
    insts = (instruments if instruments else config['trade']['instruments'])

    stream = StreamDriver(
        target=target, environment=config['oanda']['environment'],
        access_token=config['oanda']['access_token'], sqlite_path=sqlite_path,
        redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
        redis_maxl=redis_maxl
    )
    stream.fire(
        account_id=config['oanda']['account_id'], instruments=','.join(insts),
        ignore_heartbeat=True
    )
