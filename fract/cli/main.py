#!/usr/bin/env python
"""Stream and trade forex with Oanda API

Usage:
    fract init [--debug|--info] [--file=<yaml>]
    fract info [--debug|--info] [--file=<yaml>] <info_target>
    fract track [--debug|--info] [--file=<yaml>] [--sqlite=<db>]
                [--granularity=<code>] [--count=<int>] [<instrument>...]
    fract stream [--debug|--info] [--file=<yaml>] [--target=<str>]
                 [--sqlite=<db>] [--redis-host=<ip>] [--redis-port=<int>]
                 [--redis-db=<int>] [--redis-maxl=<int>] [<instrument>...]
    fract close [--debug|--info] [--file=<yaml>] [<instrument>...]
    fract open [--debug|--info] [--file=<yaml>] [--wait=<sec>] [--iter=<int>]
               [--models=<mod>] [--quiet] [<instrument>...]
    fract -h|--help
    fract -v|--version

Options:
    -h, --help          Print help and exit
    -v, --version       Print version and exit
    --debug, --info     Execute a command with debug|info messages
    --file=<yaml>       Set a path to a YAML for configurations [$FRACT_YML]
    --wait=<sec>        Wait seconds between orders [default: 0]
    --iter=<int>        Limit a number of executions
    --models=<mod>      Set trading models [default: volatility]
    --quiet             Suppress messages
    --target=<str>      Set a streaming target { rate, event } [default: rate]
    --sqlite=<db>       Save data in an SQLite3 database
    --count=<int>       Set a size for rate tracking (max: 5000) [default: 60]
    --granularity=<code>
                        Set a granularity for rate tracking [default: S5]
    --redis-host=<ip>   Set a Redis server host
    --redis-port=<int>  Set a Redis server port
    --redis-db=<int>    Set a Redis database [default: 0]
    --redis-maxl=<int>  Limit max length for records in Redis [default: 1000]

Commands:
    init                Generate a YAML template for configuration
    info                Print information about <info_target>
    track               Fetch past rates
    stream              Stream market prices or authorized account events
    close               Close positions (if not <instrument>, close all)
    open                Open autonomous trading

Arguments:
    <info_target>       { instruments, prices, account, accounts, orders,
                          trades, positions, position, transaction,
                          transaction_history, eco_calendar,
                          historical_position_ratios, historical_spreads,
                          commitments_of_traders, orderbook, autochartists }
    <instrument>        { AUD_CAD, AUD_CHF, AUD_HKD, AUD_JPY, AUD_NZD, AUD_SGD,
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
from .util import write_config_yml
from ..trade.info import print_info, track_rate
from ..trade.stream import invoke_stream
from ..trade.order import close_positions
from ..trade.auto import open_deals


def main():
    args = docopt(__doc__, version='fract {}'.format(__version__))
    _set_log_config(debug=args['--debug'], info=args['--info'])
    logger = logging.getLogger(__name__)
    logger.debug('args:{0}{1}'.format(os.linesep, args))

    if args['init']:
        write_config_yml(path=args['--file'])
    elif args['info']:
        print_info(config_yml=args['--file'], type=args['<info_target>'])
    elif args['track']:
        track_rate(
            config_yml=args['--file'], instruments=args['<instrument>'],
            granularity=args['--granularity'], count=int(args['--count']),
            sqlite_path=args['--sqlite']
        )
    elif args['stream']:
        invoke_stream(
            config_yml=args['--file'], target=args['--target'],
            instruments=args['<instrument>'], sqlite_path=args['--sqlite'],
            redis_host=args['--redis-host'], redis_port=args['--redis-port'],
            redis_db=args['--redis-db'], redis_maxl=args['--redis-maxl']
        )
    elif args['close']:
        close_positions(
            config_yml=args['--file'], instruments=args['<instrument>']
        )
    elif args['open']:
        open_deals(
            config_yml=args['--file'], instruments=args['<instrument>'],
            models=args['--models'],
            n=(int(args['--iter']) if args['--iter'] else sys.maxsize),
            interval=float(args['--wait']), quiet=args['--quiet']
        )


def _set_log_config(debug=None, info=None):
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
