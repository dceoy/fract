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


def set_config_yml(path=None, env='FRACTUS_YML', default='fractus.yml'):
    return(os.path.expanduser(tuple(filter(lambda p: p is not None,
                                           [path, os.getenv(env), default]))[0]))


def write_config_yml(path):
    if os.path.exists(path):
        print('The file already exists: {}'.format(path))
    else:
        logging.debug('Write {}'.format(path))
        with open(path, 'w') as f:
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
                    'db': {
                        'rate': 0,
                        'event': 1,
                        'trade': 2
                    },
                    'max_record': 1000
                }
            }, default_flow_style=False))
        print('A YAML template was generated: {}'.format(path))
