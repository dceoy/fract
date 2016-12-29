#!/usr/bin/env python

import logging
import numpy as np
import redis
import oandapy


class Double(oandapy.API):
    def __init__(self, account_id, instrument, config_redis, **kwargs):
        super().__init__(**kwargs)
        self.account_id = account_id
        self.instrument = instrument
        self.redis = redis.StrictRedis(host=config_redis['host'],
                                       port=config_redis['port'],
                                       db=config_redis['db']['trade'])
        self.redis.flushdb()

    def discriminate(self):
        logging.debug('Set the opening randomly: {}'.format(self.side))
        self.side = np.random.choice(['buy', 'sell'])
        self.create_order(self.account_id,
                          instrument=self.instrument,
                          units=1,
                          side=self.side,
                          type='market')


def auto(config):
    ai = Double(environment=config['oanda']['environment'],
                access_token=config['oanda']['access_token'],
                account_id=config['oanda']['account_id'],
                instrument=config['oanda']['currency_pair']['trade'],
                config_redis=config['redis'])
    ai.discriminate()
