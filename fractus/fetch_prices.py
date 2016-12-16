#!/usr/bin/env python

import oandapy
from config import read_yaml


if __name__ == '__main__':
    cf = read_yaml('../config.yml')
    cf_oanda = cf['oanda']
    oanda = oandapy.API(environment=cf_oanda['environment'],
                        access_token=cf_oanda['access_token'])
    print(oanda.get_prices(instruments=str.join(',', cf_oanda['currency_pair'])))
