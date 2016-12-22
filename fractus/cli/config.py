#!/usr/bin/env python

import logging
import os
import yaml


def set_log_config(debug=False):
    if debug:
        lv = logging.DEBUG
    else:
        lv = logging.WARNING
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=lv)


def read_yaml(yml):
    with open(yml) as f:
        dict = yaml.load(f)
    return dict


def write_config_yml(yml):
    if os.path.exists(yml):
        print('%s already exists' % yml)
    else:
        with open(yml, 'w') as f:
            f.write(yaml.dump({
                'oanda': {
                    'environment': 'live',
                    'account_id': '',
                    'access_token': '',
                    'currency_pair': [
                        'EUR_USD',
                        'GBP_USD',
                        'EUR_GBP',
                        'USD_JPY',
                        'EUR_JPY',
                        'GBP_JPY'
                    ]
                },
                'redis': {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'db': 0,
                    'max_record': 1000
                }
            }, default_flow_style=False))
