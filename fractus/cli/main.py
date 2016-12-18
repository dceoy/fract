#!/usr/bin/env python

import logging
from .arg_parser import parse_options
from .config import set_log_config, read_yaml, write_config_yml
from ..price import stream_prices


def main(config_yml='config.yml'):
    arg = parse_options(config_yml)
    set_log_config(arg['debug'])
    if arg['init']:
        logging.debug('generate config.yml')
        write_config_yml(config_yml)
    else:
        logging.debug('stream prices')
        stream_prices(read_yaml(config_yml))
