#!/usr/bin/env python

import logging
import os
import shutil
import yaml


class FractError(Exception):
    pass


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


def set_config_yml(path=None, env='FRACT_YML', default='fract.yml'):
    return os.path.expanduser(
        tuple(filter(
            lambda p: p is not None, [path, os.getenv(env), default]
        ))[0]
    )


def write_config_yml(path):
    if os.path.exists(path):
        print('The file already exists: {}'.format(path))
    else:
        logging.debug('Write {}'.format(path))
        shutil.copyfile(os.path.join(os.path.dirname(__file__),
                                     '../static/fract.yml'),
                        path)
        print('A YAML template was generated: {}'.format(path))


def fetch_executable(cmd):
    executables = tuple(filter(lambda cp: os.access(cp, os.X_OK),
                               map(lambda p: os.path.join(p, cmd),
                                   str.split(os.environ['PATH'], ':'))))
    if len(executables) == 0:
        return None
    else:
        return executables[0]


def set_redis_config(host, db, maxl, default_port=6379):
    ipp = host.split(':')
    redis_config = {
        'ip': ipp[0],
        'port': (ipp[1] if len(ipp) > 1 else default_port),
        'db': db,
        'max_llen': maxl
    }
    logging.debug('redis_config: {}'.format(redis_config))
    return redis_config
