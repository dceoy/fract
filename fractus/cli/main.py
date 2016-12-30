#!/usr/bin/env python
"""
Stream and trade forex with Oanda API

Usage:
    fract init [--debug] [--config <yaml>]
    fract info <info_type> [--debug] [--config <yaml>]
    fract rate [--debug] [--config <yaml>] [--redis]
    fract event [--debug] [--config <yaml>] [--redis]
    fract close [<instrument>...] [--debug] [--config <yaml>]
    fract auto [--debug] [--config <yaml>]
    fract -h|--help
    fract -v|--version

Options:
    -h, --help      Print help and exit
    -v, --version   Print version and exit
    --debug         Execute a command with debug messages
    --config        Set a path to a YAML for configurations [$FRACTUS_YML]
    --list          List accounts
    --redis         Store streaming data in a Redis server

Commands:
    init            Generate a YAML template for configuration
    info            Print information about <info_type>
    rate            Stream market prices
    event           Stream authorized account's events
    close           Close the positions (if not <instrument>, close all)
    auto            Trade currencies autonomously

Arguments:
    info_type       { instruments, prices, history, account, accounts,
                      orders, trades, positions, position, transaction,
                      transaction_history, eco_calendar,
                      historical_position_ratios, historical_spreads,
                      commitments_of_traders, orderbook, autochartists }
    instrument      { AUD_CAD, AUD_CHF, AUD_HKD, AUD_JPY, AUD_NZD, AUD_SGD,
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
from docopt import docopt
from .. import __version__
from .config import set_log_config, set_config_yml, write_config_yml
from .yaml import read_yaml
from ..trade import info, order, stream
from ..model import increment


def main():
    args = docopt(__doc__, version='fractus {}'.format(__version__))
    set_log_config(debug=args['--debug'])
    logging.debug('args: \n{}'.format(args))

    if args['--config']:
        config_yml = set_config_yml(path=args['<yaml>'])
    else:
        config_yml = set_config_yml()

    if args['init']:
        logging.debug('Initiation')
        write_config_yml(path=config_yml)
    else:
        logging.debug('config_yml: {}'.format(config_yml))
        config = read_yaml(path=config_yml)
        if args['info']:
            logging.debug('Information')
            info.print_info(config,
                            type=args['<info_type>'])
        elif args['rate']:
            logging.debug('Rates Streaming')
            stream.invoke(stream_type='rate',
                          config=config,
                          use_redis=args['--redis'])
        elif args['event']:
            logging.debug('Events Streaming')
            stream.invoke(stream_type='event',
                          config=config,
                          use_redis=args['--redis'])
        elif args['close']:
            logging.debug('Position Closing')
            order.close_positions(config=config,
                                  instruments=args['<instrument>'])
        elif args['auto']:
            logging.debug('Autonomous Trading')
            increment.auto(config=config)
