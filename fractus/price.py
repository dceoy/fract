#!/usr/bin/env python

import oandapy
import redis


class StreamDriver(oandapy.Streamer):
    def __init__(self, redis_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = redis.StrictRedis(host=redis_config['host'],
                                       port=redis_config['port'],
                                       db=redis_config['db'])
        self.max = redis_config['max_llen']
        self.redis.flushdb()

    def on_success(self, data):
        print(data)
        tick = data['tick']
        self.redis.rpush(tick['instrument'], tick['bid'])
        if self.redis.llen(tick['instrument']) > self.max:
            self.redis.lpop(tick['instrument'])

    def on_error(self, data):
        print(data)
        self.disconnect()


def stream_prices(config):
    stream = StreamDriver(environment=config['oanda']['environment'],
                          access_token=config['oanda']['access_token'],
                          redis_config=config['redis'])
    stream.rates(account_id=config['oanda']['account_id'],
                 instruments=str.join(',', config['oanda']['currency_pair']),
                 ignore_heartbeat=True)
