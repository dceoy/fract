#!/usr/bin/env python

import logging
import json
import signal
import oandapy
import redis


class StreamDriver(oandapy.Streamer):
    def __init__(self, config_redis, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if config_redis:
            self.redis = redis.StrictRedis(host=config_redis['host'],
                                           port=config_redis['port'],
                                           db=config_redis['db'])
            self.max = config_redis['max_llen']
            self.redis.flushdb()
        else:
            self.redis = None

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
        logging.debug('Print ticks')
        config_redis = None
    else:
        logging.debug('Print and store ticks')
        config_redis = config['redis']

    stream = StreamDriver(environment=config['oanda']['environment'],
                          access_token=config['oanda']['access_token'],
                          config_redis=config_redis)

    logging.debug('Stream prices')
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    stream.rates(account_id=config['oanda']['account_id'],
                 instruments=str.join(',', config['oanda']['currency_pair']),
                 ignore_heartbeat=True)
