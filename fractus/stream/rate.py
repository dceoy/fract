#!/usr/bin/env python

import logging
import json
import signal
import oandapy
import redis
from ..cli.config import read_yaml


class StreamDriver(oandapy.Streamer):
    def __init__(self, use_redis, config_redis, **kwargs):
        super().__init__(**kwargs)
        if use_redis:
            logging.debug('Set a tick streamer with storing data in the Redis server')
            self.redis = redis.StrictRedis(host=config_redis['host'],
                                           port=config_redis['port'],
                                           db=config_redis['db'])
            self.redis_max = config_redis['max_llen']
            self.redis.flushdb()
        else:
            logging.debug('Set a tick streamer without storing data')
            self.redis = None

    def on_success(self, data):
        print(data)
        if self.redis is not None:
            tick = data['tick']
            self.redis.rpush(tick['instrument'], json.dumps(tick))
            if self.redis.llen(tick['instrument']) > self.redis_max:
                self.redis.lpop(tick['instrument'])

    def on_error(self, data):
        logging.error(data)
        self.disconnect()


def fetch_rates(config_yml, use_redis=False):
    cf = read_yaml(config_yml)
    stream = StreamDriver(environment=cf['oanda']['environment'],
                          access_token=cf['oanda']['access_token'],
                          use_redis=use_redis,
                          config_redis=cf['redis'])
    logging.debug('Start to stream prices')
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    stream.rates(account_id=cf['oanda']['account_id'],
                 instruments=str.join(',', cf['oanda']['currency_pair']),
                 ignore_heartbeat=True)
