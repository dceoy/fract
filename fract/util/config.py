#!/usr/bin/env python

import logging
import os
import shutil


def write_config_yml(path):
    logger = logging.getLogger(__name__)
    if os.path.exists(path):
        print('A file already exists: {}'.format(path))
    else:
        logger.info('Write a config: {}'.format(path))
        shutil.copyfile(
            os.path.join(
                os.path.dirname(__file__), '../static/default_fract.yml'
            ),
            path
        )
        print('A YAML template was generated: {}'.format(path))


def fetch_config_yml_path(path=None, env='FRACT_YML', default='fract.yml'):
    logger = logging.getLogger(__name__)
    p = [
        os.path.abspath(os.path.expanduser(os.path.expandvars(p)))
        for p in [path, os.getenv(env), default] if p is not None
    ][0]
    logger.debug('abspath to a config: {}'.format(p))
    return p
