#!/usr/bin/env python
"""
Stream and trade forex with Oanda API

Usage:
    fract init [--debug] [--config <yaml>]
    fract rate [--debug] [--config <yaml>] [--redis]
    fract event [--debug] [--config <yaml>] [--redis]
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
    rate            Stream market prices
    event           Stream authorized account's events
    trade           Trade currencies with a simple algorithm
"""

import logging
from docopt import docopt
from .config import set_log_config, set_config_yml, write_config_yml
from ..stream import streamer
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
    elif args['rate']:
        logging.debug('Rates Streaming')
        streamer.invoke(stream_type='rate',
                        config_yml=config_yml,
                        use_redis=args['--redis'])
    elif args['event']:
        logging.debug('Events Streaming')
        streamer.invoke(stream_type='event',
                        config_yml=config_yml,
                        use_redis=args['--redis'])
    elif args['trade']:
        pass
