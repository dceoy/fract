#!/usr/bin/env python

import logging
import json
import os
import signal
import sqlite3
import subprocess
import oandapy
import redis
from ..cli.util import fetch_executable


class StreamDriver(oandapy.Streamer):
    def __init__(self, target, sqlite_path=None, redis_config=None, **kwargs):
        super().__init__(**kwargs)
        self.target = target
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.target]
        if sqlite_path:
            logging.debug('Set a streamer with SQLite')
            if not os.path.isfile(sqlite_path):
                subprocess.run(
                    '{0} {1} ".read {2}"'.format(
                        fetch_executable('sqlite3'),
                        sqlite_path,
                        os.path.join(
                            os.path.dirname(__file__),
                            '../static/create_tables.sql'
                        )
                    ),
                    shell=True
                )
            self.sqlite = sqlite3.connect(sqlite_path)
        else:
            self.sqlite = None
        if redis_config:
            logging.debug('Set a streamer with Redis')
            self.redis = redis.StrictRedis(host=redis_config['ip'],
                                           port=redis_config['port'],
                                           db=redis_config['db'])
            self.redis_max = redis_config['max_llen']
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
            if self.redis.llen(instrument) > self.redis_max:
                self.redis.lpop(instrument)
        if 'disconnect' in data:
            self.disconnect()
            if self.sqlite:
                self.sqlite.close()
            if self.redis:
                self.redis.connection_pool.disconnect()

    def on_error(self, data):
        logging.error(data)
        self.disconnect()
        if self.sqlite:
            self.sqlite.close()
        if self.redis:
            self.redis.connection_pool.disconnect()

    def fire(self, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.target == 'rate':
            logging.debug('Start to stream market prices')
            self.rates(**kwargs)
        elif self.target == 'event':
            logging.debug('Start to stream authorized account\'s events')
            self.events(**kwargs)


def invoke(target, instruments, config, sqlite_path, redis_config):
    insts = (instruments if instruments else config['trade']['instruments'])
    stream = StreamDriver(
        target=target,
        environment=config['oanda']['environment'],
        access_token=config['oanda']['access_token'],
        sqlite_path=sqlite_path,
        redis_config=redis_config
    )
    stream.fire(
        account_id=config['oanda']['account_id'],
        instruments=','.join(insts),
        ignore_heartbeat=True
    )
