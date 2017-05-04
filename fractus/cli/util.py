#!/usr/bin/env python

import logging
import os
import yaml


def set_log_config(debug=False):
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG if debug else logging.WARNING)


def read_yaml(path):
    with open(path) as f:
        d = yaml.load(f)
    return d


def dump_yaml(dict, flow=False):
    return yaml.dump(dict, default_flow_style=flow)


def set_config_yml(path=None, env='FRACTUS_YML', default='fractus.yml'):
    return(os.path.expanduser(tuple(filter(
        lambda p: p is not None, [path, os.getenv(env), default]
    ))[0]))


def write_config_yml(path):
    if os.path.exists(path):
        print('The file already exists: {}'.format(path))
    else:
        logging.debug('Write {}'.format(path))
        with open(path, 'w') as f:
            f.write(dump_yaml({
                'oanda': {
                    'environment': 'live',
                    'account_id': '',
                    'access_token': ''
                },
                'redis': {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'db': {
                        'rate': 0,
                        'event': 1
                    },
                    'max_record': 1000
                },
                'model': {
                    'increment': {
                        'unit': {'opening': 1, 'increment': 1},
                        'stop': {'loss': 0.01, 'trail': 0.02},
                        'interval': 1
                    }
                }
            }))
        print('A YAML template was generated: {}'.format(path))
