fract
=====

Automated Trading Framework using Oanda API

[![wercker status](https://app.wercker.com/status/cd7eaa54e3874264bb4745dc0d3b7484/m/master "wercker status")](https://app.wercker.com/project/byKey/cd7eaa54e3874264bb4745dc0d3b7484)

Installation
------------

```sh
$ pip install -U https://github.com/oanda/oandapy/archive/master.tar.gz \
                 https://github.com/dceoy/fract/archive/master.tar.gz
```

Usage
-----

```sh
$ fract --help
Stream, track, and trade forex with Oanda API

Usage:
    fract -h|--help
    fract -v|--version
    fract init [--debug|--info] [--file=<yaml>]
    fract info [--debug|--info] [--file=<yaml>] <info_target> [<instrument>...]
    fract track [--debug|--info] [--file=<yaml>] [--sqlite=<path>]
                [--granularity=<code>] [--count=<int>] [<instrument>...]
    fract stream [--debug|--info] [--file=<yaml>] [--target=<str>]
                 [--sqlite=<path>] [--redis-host=<ip>] [--redis-port=<int>]
                 [--redis-db=<int>] [--redis-max-llen=<int>] [--quiet]
                 [<instrument>...]
    fract open [--debug|--info] [--file=<yaml>] [--model=<str>]
               [--interval=<sec>] [--timeout=<sec>] [--with-streamer]
               [--redis-host=<ip>] [--redis-port=<int>] [--redis-db=<int>]
               [--log-dir=<path>] [--quiet] [<instrument>...]
    fract close [--debug|--info] [--file=<yaml>] [<instrument>...]

Options:
    -h, --help          Print help and exit
    -v, --version       Print version and exit
    --debug, --info     Execute a command with debug|info messages
    --file=<yaml>       Set a path to a YAML for configurations [$FRACT_YML]
    --quiet             Suppress messages
    --sqlite=<path>     Save data in an SQLite3 database
    --granularity=<code>
                        Set a granularity for rate tracking [default: S5]
    --count=<int>       Set a size for rate tracking (max: 5000) [default: 60]
    --target=<str>      Set a streaming target { rate, event } [default: rate]
    --redis-host=<ip>   Set a Redis server host (override YAML configurations)
    --redis-port=<int>  Set a Redis server port (override YAML configurations)
    --redis-db=<int>    Set a Redis database (override YAML configurations)
    --redis-max-llen=<int>
                        Limit Redis list length (override YAML configurations)
    --model=<str>       Set trading models [default: ewm]
    --interval=<sec>    Wait seconds between iterations [default: 0]
    --timeout=<sec>     Set senconds for response timeout [default: 3600]
    --with-streamer     Invoke a trader with a streamer
    --log-dir=<path>    Write output log files in a directory

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
```
