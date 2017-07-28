#!/usr/bin/env python
"""
Stream and trade forex with Oanda API

Usage:
    fract init [--debug] [--file=<yaml>]
    fract info [--debug] [--file=<yaml>] <info_type>
    fract rate [--debug] [--file=<yaml>] [--use-redis] [--redis-db=<int>]
               [--redis-host=<ip_port>] [--redis-maxl=<int>]
               [<instrument>...]
    fract event [--debug] [--file=<yaml>] [--use-redis] [--redis-db=<int>]
                [--redis-host=<ip:port>] [--redis-maxl=<int>]
                [<instrument>...]
    fract close [--debug] [--file=<yaml>] [<instrument>...]
    fract open [--debug] [--file=<yaml>] [--wait=<sec>] [--iter=<num>]
               [--models=<mod>] [--quiet] [<instrument>...]
    fract -h|--help
    fract -v|--version

Options:
    -h, --help      Print help and exit
    -v, --version   Print version and exit
    --debug         Execute a command with debug messages
    --file=<yaml>   Set a path to a YAML for configurations [$FRACT_YML]
    --wait=<sec>    Wait seconds between orders [default: 0]
    --iter=<num>    Limit a number of executions
    --models=<mod>  Set trading models (comma-separated) [default: volatility]
    --quiet         Suppress messages
    --use-redis     Store streaming data in a Redis server
    --redis-host=<ip:port>
                    Set a Redis server host [default: 127.0.0.1:6379]
    --redis-db=<int>
                    Set a Redis database [default: 0]
    --redis-maxl=<int>
                    Limit max length for records in Redis [default: 1000]

Commands:
    init            Generate a YAML template for configuration
    info            Print information about <info_type>
    rate            Stream market prices
    event           Stream authorized account's events
    close           Close positions (if not <instrument>, close all)
    open            Open autonomous trading

Arguments:
    <info_type>     { instruments, prices, history, account, accounts,
                      orders, trades, positions, position, transaction,
                      transaction_history, eco_calendar,
                      historical_position_ratios, historical_spreads,
                      commitments_of_traders, orderbook, autochartists }
    <instrument>    { AUD_CAD, AUD_CHF, AUD_HKD, AUD_JPY, AUD_NZD, AUD_SGD,
                      AUD_USD, CAD_CHF, CAD_HKD, CAD_JPY, CAD_SGD, CHF_HKD,
                      CHF_JPY, CHF_ZAR, EUR_AUD, EUR_CAD, EUR_CHF, EUR_CZK,
                      EUR_DKK, EUR_GBP, EUR_HKD, EUR_HUF, EUR_JPY, EUR_NOK,
                      EUR_NZD, EUR_PLN, EUR_SEK, EUR_SGD, EUR_TRY, EUR_USD,
                      EUR_ZAR, GBP_AUD, GBP_CAD, GBP_CHF, GBP_HKD, GBP_JPY,
                      GBP_NZD, GBP_PLN, GBP_SGD, GBP_USD, GBP_ZAR, HKD_JPY,
                      NZD_CAD, NZD_CHF, NZD_HKD, NZD_JPY, NZD_SGD, NZD_USD,
                      SGD_CHF, SGD_HKD, SGD_JPY, TRY_JPY, USD_CAD, USD_CHF,
                      USD_CNH, USD_CZK, USD_DKK, USD_HKD, USD_HUF, USD_INR,
                      USD_JPY, USD_MXN, USD_NOK, USD_PLN, USD_SAR, USD_SEK,
                      USD_SGD, USD_THB, USD_TRY, USD_ZAR, ZAR_JPY }
"""

import logging
import os
import sys
from docopt import docopt
from .. import __version__
from .util import set_log_config, set_config_yml, write_config_yml, read_yaml, \
                  set_redis_config
from ..trade import info, order, stream, auto


def main():
    args = docopt(__doc__, version='fract {}'.format(__version__))
    set_log_config(debug=args['--debug'])
    logging.debug('args:{0}{1}'.format(os.linesep, args))
    config_yml = set_config_yml(path=args['--file'])
    redis_config = (
        set_redis_config(host=args['--redis-host'],
                         db=args['--redis-db'],
                         maxl=args['--redis-maxl'])
        if args['--use-redis'] else None
    )

    if args['init']:
        logging.debug('Initiation')
        write_config_yml(path=config_yml)
    else:
        logging.debug('config_yml: {}'.format(config_yml))
        config = read_yaml(path=config_yml)
        if args['info']:
            logging.debug('Information')
            info.print_info(
                config,
                type=args['<info_type>']
            )
        elif args['rate']:
            logging.debug('Rates Streaming')
            stream.invoke(
                stream_type='rate',
                instruments=args['<instrument>'],
                config=config,
                redis_config=redis_config
            )
        elif args['event']:
            logging.debug('Events Streaming')
            stream.invoke(
                stream_type='event',
                instruments=args['<instrument>'],
                config=config,
                redis_config=redis_config
            )
        elif args['close']:
            logging.debug('Position Closing')
            order.close_positions(
                config=config,
                instruments=args['<instrument>']
            )
        elif args['open']:
            logging.debug('Autonomous Trading')
            auto.open_deals(
                config=config,
                instruments=args['<instrument>'],
                models=args['--models'],
                n=(int(args['--iter']) if args['--iter'] else sys.maxsize),
                interval=float(args['--wait']),
                quiet=args['--quiet']
            )
