#!/usr/bin/env python

import logging
import json
import oandapy
import redis


class StreamDriver(oandapy.Streamer):
    def __init__(self, config_redis, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if config_redis is None:
            self.redis = None
        else:
            self.redis = redis.StrictRedis(host=config_redis['host'],
                                           port=config_redis['port'],
                                           db=config_redis['db'])
            self.max = config_redis['max_llen']
            self.redis.flushdb()

    def on_success(self, data):
        print(data)
        if self.redis is not None:
            tick = data['tick']
            self.redis.rpush(tick['instrument'], json.dumps(tick))
            if self.redis.llen(tick['instrument']) > self.max:
                self.redis.lpop(tick['instrument'])

    def on_error(self, data):
        logging.error(data)
        self.disconnect()


def stream_prices(config, print_only):
    if print_only:
        config_redis = None
    else:
        config_redis = config['redis']
    stream = StreamDriver(environment=config['oanda']['environment'],
                          access_token=config['oanda']['access_token'],
                          config_redis=config_redis)
    stream.rates(account_id=config['oanda']['account_id'],
                 instruments=str.join(',', config['oanda']['currency_pair']),
                 ignore_heartbeat=True)
