#!/usr/bin/env python

import yaml
import oandapy


def read_yaml(yml):
    with open(yml) as f:
        dict = yaml.load(f)
    return dict


if __name__ == '__main__':
    cfg = read_yaml('config.yml')
    oanda = oandapy.API(environment=cfg['environment'], access_token=cfg['oanda_token'])
    cp = str.join(',', cfg['instruments'])
    print(oanda.get_prices(instruments=cp))
