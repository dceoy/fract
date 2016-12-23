#!/usr/bin/env python
"""
Stream and trade forex with Oanda API

Usage:
    fract init [--debug] [--config <yaml>]
    fract stream [--debug] [--config <yaml>] [--redis]
    fract trade [--debug] [--config <yaml>]
    fract -h|--help
    fract -v|--version

Options:
    -h, --help      Print help and exit
    -v, --version   Print version and exit
    --debug         Execute a command with debug messages
    --config        Set a path to a YAML for configurations [$FRACTUS_YML]
    --redis         Store streaming data in a Redis server

Commands:
    init            Generate a YAML template for configuration
    stream          Streming prices
    trade           Trade currencies with a simple algorithm
"""

import logging
from docopt import docopt
from .config import set_log_config, set_config_yml, write_config_yml
from ..stream.rate import fetch_rates
from .. import __version__


def main():
    args = docopt(__doc__, version='fractus {}'.format(__version__))
    set_log_config(debug=args['--debug'])
    logging.debug('args: \n{}'.format(args))

    if args['--config']:
        config_yml = set_config_yml(path=args['<yaml>'])
    else:
        config_yml = set_config_yml()
    logging.debug('config_yml: {}'.format(config_yml))

    if args['init']:
        logging.debug('Initiation')
        write_config_yml(path=config_yml)
    elif args['stream']:
        logging.debug('Streaming')
        fetch_rates(config_yml=config_yml,
                    use_redis=args['--redis'])
    elif args['trade']:
        logging.debug('Trading')
        pass
