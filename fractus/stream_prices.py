#!/usr/bin/env python

import oandapy
from config import read_yaml


class StreamDriver(oandapy.Streamer):
    def __init__(self, count=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.count = count
        self.reccnt = 0

    def on_success(self, data):
        print(data)
        self.reccnt += 1
        if self.reccnt == self.count:
            self.disconnect()

    def on_error(self, data):
        print(data)
        self.disconnect()


if __name__ == '__main__':
    cf = read_yaml('../config.yml')
    stream = StreamDriver(environment=cf['environment'],
                          access_token=cf['oanda_token'])
    stream.rates(account_id=cf['oanda_account_id'],
                 instruments=str.join(',', cf['currency_pair']),
                 ignore_heartbeat=True)
