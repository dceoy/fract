#!/usr/bin/env python
"""
Stream and trade forex with Oanda API

Usage:
    fract init [--debug]
    fract stream [--debug] [--print-only]
    fract trade [--debug]
    fract -h|--help
    fract --version

Options:
    -h, --help      Print help and exit
    --version       Print version and exit
    --debug         Execute a command with debug messages

Commands:
    init            Generate `config.yml` as a template for configuration
    stream          Streming prices and record them to a Redis server
    trade           Trade currencies with a simple algorithm
"""

import logging
import signal
from docopt import docopt
from .config import set_log_config, read_yaml, write_config_yml
from ..price.streaming import stream_prices
from .. import __version__


def main(config_yml='config.yml'):
    args = docopt(__doc__, version='fractus {}'.format(__version__))
    set_log_config(debug=args['--debug'])
    if args['init']:
        logging.debug('generate config.yml')
        write_config_yml(yml=config_yml)
    elif args['stream']:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        logging.debug('stream prices')
        stream_prices(config=read_yaml(config_yml),
                      print_only=args['--print-only'])
    elif args['trade']:
        logging.debug('trade currencies with a simple algorithm')
        pass
