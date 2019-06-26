#!/usr/bin/env python
"""
Automated Trader using Oanda V20 REST API

Usage:
    fract -h|--help
    fract -v|--version
    fract init [--debug|--info] [--file=<yaml>]
    fract info [--debug|--info] [--file=<yaml>] [--json] <info_target>
               [<instrument>...]
    fract track [--debug|--info] [--file=<yaml>] [--csv-dir=<path>]
                [--sqlite=<path>] [--granularity=<code>] [--count=<int>]
                [--json] [--quiet] [<instrument>...]
    fract stream [--debug|--info] [--file=<yaml>] [--target=<str>]
                 [--timeout=<sec>] [--csv=<path>] [--sqlite=<path>]
                 [--use-redis] [--redis-host=<ip>] [--redis-port=<int>]
                 [--redis-db=<int>] [--redis-max-llen=<int>]
                 [--ignore-api-error] [--quiet] [<instrument>...]
    fract close [--debug|--info] [--file=<yaml>] [<instrument>...]
    fract open [--debug|--info] [--file=<yaml>] [--model=<str>]
               [--interval=<sec>] [--timeout=<sec>] [--standalone]
               [--redis-host=<ip>] [--redis-port=<int>] [--redis-db=<int>]
               [--log-dir=<path>] [--ignore-api-error] [--quiet] [--dry-run]
               [<instrument>...]

Options:
    -h, --help          Print help and exit
    -v, --version       Print version and exit
    --debug, --info     Execute a command with debug|info messages
    --file=<yaml>       Set a path to a YAML for configurations [$OANDA_YML]
    --quiet             Suppress messages
    --csv-dir=<path>    Write data with daily CSV in a directory
    --sqlite=<path>     Save data in an SQLite3 database
    --granularity=<code>
                        Set a granularity for rate tracking [default: S5]
    --count=<int>       Set a size for rate tracking (max: 5000) [default: 60]
    --json              Print data with JSON
    --target=<str>      Set a streaming target [default: pricing]
                        { pricing, transaction }
    --timeout=<sec>     Set senconds for response timeout
    --csv=<path>        Write data with CSV into a file
    --use-redis         Use Redis for data store
    --redis-host=<ip>   Set a Redis server host (override YAML configurations)
    --redis-port=<int>  Set a Redis server port (override YAML configurations)
    --redis-db=<int>    Set a Redis database (override YAML configurations)
    --redis-max-llen=<int>
                        Limit Redis list length (override YAML configurations)
    --ignore-api-error  Ignore Oanda API connection errors
    --model=<str>       Set trading models [default: ewma]
    --interval=<sec>    Wait seconds between iterations [default: 0]
    --standalone        Invoke a trader with standalone mode
    --log-dir=<path>    Write output log files in a directory
    --dry-run           Invoke a trader with dry-run mode

Commands:
    init                Create a YAML template for configuration
    info                Print information about <info_target>
    track               Fetch past rates
    stream              Stream market prices or authorized account events
    close               Close positions (if not <instrument>, close all)
    open                Invoke an autonomous trader

Arguments:
    <info_target>       { instruments, prices, account, accounts, orders,
                          trades, positions, position, transactions,
                          order_book, position_book }
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

from docopt import docopt
from oandacli.cli.main import execute_command
from oandacli.util.config import fetch_config_yml_path, write_config_yml
from oandacli.util.logger import set_log_config

from .. import __version__
from ..call.trader import invoke_trader


def main():
    args = docopt(__doc__, version='fract {}'.format(__version__))
    set_log_config(debug=args['--debug'], info=args['--info'])
    logger = logging.getLogger(__name__)
    logger.debug('args:{0}{1}'.format(os.linesep, args))
    config_yml_path = fetch_config_yml_path(
        path=args['--file'], env='FRACT_YML', default='fract.yml'
    )
    if args['init']:
        write_config_yml(
            dest_path=config_yml_path,
            template_path=os.path.join(
                os.path.dirname(__file__), '../static/default_fract.yml'
            )
        )
    elif args['open']:
        invoke_trader(
            config_yml=config_yml_path, instruments=args['<instrument>'],
            model=args['--model'], interval_sec=args['--interval'],
            timeout_sec=args['--timeout'], standalone=args['--standalone'],
            redis_host=args['--redis-host'], redis_port=args['--redis-port'],
            redis_db=args['--redis-db'], log_dir_path=args['--log-dir'],
            ignore_api_error=args['--ignore-api-error'], quiet=args['--quiet'],
            dry_run=args['--dry-run']
        )
    else:
        execute_command(args=args, config_yml_path=config_yml_path)
