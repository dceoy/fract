#!/usr/bin/env python

import oandapy
import redis
from config import read_yaml


class StreamDriver(oandapy.Streamer):
    def __init__(self, redis_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = redis_config

    def on_success(self, data):
        print(data)
        tick = data['tick']
        r = redis.StrictRedis(host=self.redis['host'],
                              port=self.redis['port'],
                              db=self.redis['db'])
        r.rpush(tick['instrument'], tick['bid'])

    def on_error(self, data):
        print(data)
        self.disconnect()


if __name__ == '__main__':
    cf = read_yaml('../config.yml')
    cf_oanda = cf['oanda']
    stream = StreamDriver(environment=cf_oanda['environment'],
                          access_token=cf_oanda['access_token'],
                          redis_config=cf['redis'])
    stream.rates(account_id=cf_oanda['account_id'],
                 instruments=str.join(',', cf_oanda['currency_pair']),
                 ignore_heartbeat=True)
