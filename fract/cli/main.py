#!/usr/bin/env python
"""Stream, track, and trade forex with Oanda API

Usage:
    fract -h|--help
    fract -v|--version
    fract init [--debug|--info] [--file=<yaml>]
    fract info [--debug|--info] [--file=<yaml>] [--json] <info_target>
               [<instrument>...]
    fract track [--debug|--info] [--file=<yaml>] [--sqlite=<path>]
                [--granularity=<code>] [--count=<int>] [--json] [--quiet]
                [<instrument>...]
    fract stream [--debug|--info] [--file=<yaml>] [--target=<str>]
                 [--csv=<path>] [--sqlite=<path>] [--use-redis]
                 [--redis-host=<ip>] [--redis-port=<int>] [--redis-db=<int>]
                 [--redis-max-llen=<int>] [--json] [--quiet] [<instrument>...]
    fract open [--debug|--info] [--file=<yaml>] [--model=<str>]
               [--interval=<sec>] [--timeout=<sec>] [--redis-host=<ip>]
               [--redis-port=<int>] [--redis-db=<int>] [--log-dir=<path>]
               [--quiet] [--dry-run] [<instrument>...]
    fract close [--debug|--info] [--file=<yaml>] [<instrument>...]

Options:
    -h, --help          Print help and exit
    -v, --version       Print version and exit
    --debug, --info     Execute a command with debug|info messages
    --file=<yaml>       Set a path to a YAML for configurations [$FRACT_YML]
    --quiet             Suppress messages
    --csv=<path>        Write data in a CSV or TSV file
    --sqlite=<path>     Save data in an SQLite3 database
    --granularity=<code>
                        Set a granularity for rate tracking [default: S5]
    --count=<int>       Set a size for rate tracking (max: 5000) [default: 60]
    --json              Print data with JSON
    --target=<str>      Set a streaming target { rate, event } [default: rate]
    --use-redis         Use Redis for data store
    --redis-host=<ip>   Set a Redis server host (override YAML configurations)
    --redis-port=<int>  Set a Redis server port (override YAML configurations)
    --redis-db=<int>    Set a Redis database (override YAML configurations)
    --redis-max-llen=<int>
                        Limit Redis list length (override YAML configurations)
    --model=<str>       Set trading models [default: ewma]
    --interval=<sec>    Wait seconds between iterations [default: 0]
    --timeout=<sec>     Set senconds for response timeout
    --log-dir=<path>    Write output log files in a directory
    --dry-run           Invoke a trader with dry-run mode

Commands:
    init                Generate a YAML template for configuration
    info                Print information about <info_target>
    track               Fetch past rates
    stream              Stream market prices or authorized account events
    open                Invoke an autonomous trader
    close               Close positions (if not <instrument>, close all)

Arguments:
    <info_target>       { instruments, prices, account, accounts, orders,
                          trades, positions, position, transaction_history,
                          eco_calendar, historical_position_ratios,
                          historical_spreads, commitments_of_traders,
                          orderbook, autochartists }
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
from .. import __version__
from ..call.candle import track_rate
from ..call.info import print_info
from ..call.order import close_positions
from ..call.streamer import invoke_streamer
from ..call.trader import invoke_trader
from ..util.config import write_config_yml
from ..util.logger import set_log_config


def main():
    args = docopt(__doc__, version='fract {}'.format(__version__))
    set_log_config(debug=args['--debug'], info=args['--info'])
    logger = logging.getLogger(__name__)
    logger.debug('args:{0}{1}'.format(os.linesep, args))
    if args['init']:
        write_config_yml(path=args['--file'])
    elif args['info']:
        print_info(
            config_yml=args['--file'], instruments=args['<instrument>'],
            type=args['<info_target>'], print_json=args['--json']
        )
    elif args['track']:
        track_rate(
            config_yml=args['--file'], instruments=args['<instrument>'],
            granularity=args['--granularity'], count=args['--count'],
            sqlite_path=args['--sqlite'], print_json=args['--json'],
            quiet=args['--quiet']
        )
    elif args['stream']:
        invoke_streamer(
            config_yml=args['--file'], target=args['--target'],
            instruments=args['<instrument>'], print_json=args['--json'],
            csv_path=args['--csv'], sqlite_path=args['--sqlite'],
            use_redis=args['--use-redis'], redis_host=args['--redis-host'],
            redis_port=args['--redis-port'], redis_db=args['--redis-db'],
            redis_max_llen=args['--redis-max-llen'], quiet=args['--quiet']
        )
    elif args['open']:
        invoke_trader(
            config_yml=args['--file'], instruments=args['<instrument>'],
            model=args['--model'], interval_sec=args['--interval'],
            timeout_sec=args['--timeout'], redis_host=args['--redis-host'],
            redis_port=args['--redis-port'], redis_db=args['--redis-db'],
            log_dir_path=args['--log-dir'], quiet=args['--quiet'],
            dry_run=args['--dry-run']
        )
    elif args['close']:
        close_positions(
            config_yml=args['--file'], instruments=args['<instrument>']
        )
