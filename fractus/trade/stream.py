#!/usr/bin/env python

import logging
import json
import signal
import oandapy
import redis


class StreamDriver(oandapy.Streamer):
    def __init__(self, stream_type, use_redis, config_redis, **kwargs):
        super().__init__(**kwargs)
        self.type = stream_type
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.type]
        if use_redis:
            logging.debug('Set a streamer with Redis')
            self.redis = redis.StrictRedis(host=config_redis['host'],
                                           port=config_redis['port'],
                                           db=config_redis['db'][self.type])
            self.redis_max = config_redis['max_llen']
            self.redis.flushdb()
        else:
            logging.debug('Set a streamer')
            self.redis = None

    def on_success(self, data):
        print(data)
        if self.redis is not None:
            instrument = data[self.key]['instrument']
            self.redis.rpush(instrument, json.dumps(data))
            if self.redis.llen(instrument) > self.redis_max:
                self.redis.lpop(instrument)

    def on_error(self, data):
        logging.error(data)
        self.disconnect()

    def fire(self, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.type == 'rate':
            logging.debug('Start to stream market prices')
            self.rates(**kwargs)
        elif self.type == 'event':
            logging.debug('Start to stream authorized account\'s events')
            self.events(**kwargs)


def invoke(stream_type, config, use_redis=False):
    stream = StreamDriver(stream_type=stream_type,
                          environment=config['oanda']['environment'],
                          access_token=config['oanda']['access_token'],
                          use_redis=use_redis,
                          config_redis=config['redis'])
    stream.fire(account_id=config['oanda']['account_id'],
                instruments=str.join(',',
                                     config['oanda']['currency_pair']),
                ignore_heartbeat=True)
