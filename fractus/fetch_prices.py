#!/usr/bin/env python

import oandapy
from config import read_yaml


if __name__ == '__main__':
    cfg = read_yaml('../config.yml')
    oanda = oandapy.API(environment=cfg['environment'],
                        access_token=cfg['oanda_token'])
    print(oanda.get_prices(instruments=str.join(',', cfg['currency_pair'])))
