#!/usr/bin/env python

from datetime import datetime
import logging
import os
import shutil
import time
import yaml


class FractError(RuntimeError):
    pass


def set_log_config(debug=None, info=None):
    if debug:
        lv = logging.DEBUG
    elif info:
        lv = logging.INFO
    else:
        lv = logging.WARNING
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', level=lv
    )


def read_config_yml(path):
    with open(_config_yml_path(path=path)) as f:
        d = yaml.load(f)
    return d


def write_config_yml(path):
    logger = logging.getLogger(__name__)
    p = _config_yml_path(path=path)
    if os.path.exists(p):
        print('A file already exists: {}'.format(p))
    else:
        logger.info('Write a config: {}'.format(p))
        shutil.copyfile(
            os.path.join(
                os.path.dirname(__file__), '../static/default_fract.yml'
            ),
            _config_yml_path(path=p)
        )
        print('A YAML template was generated: {}'.format(p))


def _config_yml_path(path=None, env='FRACT_YML', default='fract.yml'):
    logger = logging.getLogger(__name__)
    p = os.path.abspath(os.path.expanduser(
        [p for p in [path, os.getenv(env), default] if p is not None][0]
    ))
    logger.debug('abspath to a config: {}'.format(p))
    return p


def wait(from_datetime, sec=0.5):
    rest = sec - (datetime.now() - from_datetime).total_seconds()
    if rest > 0:
        time.sleep(secs=rest)
