fract
=====

Automated Trading Framework using Oanda API

[![wercker status](https://app.wercker.com/status/cd7eaa54e3874264bb4745dc0d3b7484/m/master "wercker status")](https://app.wercker.com/project/byKey/cd7eaa54e3874264bb4745dc0d3b7484)

Installation
------------

```sh
$ pip install -U git+https://github.com/oanda/oandapy.git \
                 git+https://github.com/dceoy/fract.git
```

Usage
-----

```sh
$ fract --help
Stream and trade forex with Oanda API

Usage:
    fract init [--debug] [--file=<yaml>]
    fract info [--debug] [--file=<yaml>] <info_type>
    fract track [--debug] [--file=<yaml>] [--sqlite=<db>] [--json=<name>]
                [--granularity=<code>] [--count=<int>] [<instrument>...]
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
    --sqlite=<db>   Save data in an SQLite3 database
    --json=<name>   Save data as a JSON file
    --count=<int>   Set a size for rate tracking (max: 5000) [default: 12]
    --granularity=<code>
                    Set a granularity for rate tracking [default: S5]
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
    track           Fetch past rates
    rate            Stream market prices
    event           Stream events for an authorized account
    close           Close positions (if not <instrument>, close all)
    open            Open autonomous trading

Arguments:
    <info_type>     { instruments, prices, account, accounts, orders, trades,
                      positions, position, transaction, transaction_history,
                      eco_calendar, historical_position_ratios,
                      historical_spreads, commitments_of_traders, orderbook,
                      autochartists }
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
```
