#!/usr/bin/env python
"""
Stream and trade forex with Oanda API

Usage:
    fract init [--debug] [--config <yaml>]
    fract stream [--debug] [--config <yaml>] [--print-only]
    fract trade [--debug] [--config <yaml>]
    fract -h|--help
    fract -v|--version

Options:
    -h, --help      Print help and exit
    -v, --version   Print version and exit
    --debug         Execute a command with debug messages
    --config        Set a path to a YAML for configurations (default:a ./fractus.yml)
    --print-only    Stream prices without storing

Commands:
    init            Generate a YAML template for configuration
    stream          Streming prices and record them to a Redis server
    trade           Trade currencies with a simple algorithm
"""

import logging
import os
from docopt import docopt
from .config import set_log_config, read_yaml, write_config_yml
from ..price.streaming import stream_prices
from .. import __version__


def main():
    args = docopt(__doc__, version='fractus {}'.format(__version__))
    set_log_config(debug=args['--debug'])

    if args['--config']:
        config_yml = os.path.expanduser(args['--config'])
    elif os.getenv('FRACTUS_YML'):
        config_yml = os.path.expanduser(os.getenv('FRACTUS_YML'))
    else:
        config_yml = './fractus.yml'

    if args['init']:
        write_config_yml(path=config_yml)
    elif args['stream']:
        stream_prices(config=read_yaml(config_yml),
                      print_only=args['--print-only'])
    elif args['trade']:
        logging.debug('Trade currencies with a simple algorithm')
        pass
