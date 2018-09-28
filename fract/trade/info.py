#!/usr/bin/env python

import json
import os
import logging
import sqlite3
import oandapy
import pandas as pd
import pandas.io.sql as pdsql
import yaml
from ..cli.util import FractError, read_config_yml


def track_rate(config_yml, instruments, granularity, count, sqlite_path=None):
    logger = logging.getLogger(__name__)
    logger.info('Rate tracking')
    cf = read_config_yml(path=config_yml)
    oanda = oandapy.API(
        environment=cf['oanda']['environment'],
        access_token=cf['oanda']['access_token']
    )
    candles = {
        inst: [
            d for d
            in oanda.get_history(
                account_id=cf['oanda']['account_id'], instrument=inst,
                candleFormat='bidask', granularity=granularity,
                count=int(count)
            )['candles'] if d['complete']
        ] for inst in (instruments or cf['instruments'])
    }
    if sqlite_path:
        df = pd.concat([
            pd.DataFrame.from_dict(
                d
            ).drop(
                ['complete'], axis=1
            ).assign(
                instrument=i
            )
            for i, d in candles.items()
        ]).reset_index(
            drop=True
        )
        logger.debug('df.shape: {}'.format(df.shape))
        if os.path.isfile(sqlite_path):
            with sqlite3.connect(sqlite_path) as con:
                df_diff = df.merge(
                    pdsql.read_sql(
                        'SELECT instrument, time FROM candle;', con
                    ).assign(
                        in_db=True
                    ),
                    on=['instrument', 'time'], how='left'
                ).pipe(
                    lambda d: d[d['in_db'].isnull()].drop(['in_db'], axis=1)
                ).reset_index(
                    drop=True
                )
                logger.debug('df_diff:{0}{1}'.format(os.linesep, df_diff))
                pdsql.to_sql(
                    df_diff, 'candle', con, index=False, if_exists='append'
                )
        else:
            with open(os.path.join(os.path.dirname(__file__),
                                   '../static/create_tables.sql'),
                      'r') as f:
                sql = f.read()
            with sqlite3.connect(sqlite_path) as con:
                con.executescript(sql)
                logger.debug('df:{0}{1}'.format(os.linesep, df))
                pdsql.to_sql(
                    df, 'candle', con, index=False, if_exists='append'
                )
    else:
        print(json.dumps(candles))


def print_info(config_yml, instruments=[], type='accounts'):
    logger = logging.getLogger(__name__)
    available_types = [
        'instruments', 'account', 'accounts', 'orders', 'trades', 'positions',
        'transactions', 'prices', 'position', 'eco_calendar',
        'historical_position_ratios', 'historical_spreads',
        'commitments_of_traders', 'orderbook', 'autochartist',
    ]
    if type not in available_types:
        raise FractError('invalid info type: {}'.format(type))
    logger.info('Information')
    cf = read_config_yml(path=config_yml)
    oanda = oandapy.API(
        environment=cf['oanda']['environment'],
        access_token=cf['oanda']['access_token']
    )
    account_id = cf['oanda']['account_id']
    insts_str = ','.join(cf.get('instruments') or instruments)
    period = 604800     # 1 weeik
    arg_insts = {'instruments': insts_str} if insts_str else {}
    if type == 'instruments':
        res = oanda.get_instruments(
            account_id=account_id,
            fields=','.join([
                'displayName', 'pip', 'maxTradeUnits', 'precision',
                'maxTrailingStop', 'minTrailingStop', 'marginRate', 'halted'
            ]),
            **arg_insts
        )
    elif type == 'account':
        res = oanda.get_account(account_id=account_id)
    elif type == 'accounts':
        res = oanda.get_accounts()
    elif type == 'orders':
        res = oanda.get_orders(account_id=account_id, **arg_insts)
    elif type == 'trades':
        res = oanda.get_trades(account_id=account_id, **arg_insts)
    elif type == 'positions':
        res = oanda.get_positions(account_id=account_id)
    elif type == 'transactions':
        res = oanda.get_transaction_history(account_id=account_id, **arg_insts)
    elif not insts_str:
        raise FractError('{}: instruments required'.format(type))
    elif type == 'prices':
        res = oanda.get_prices(account_id=account_id, instruments=insts_str)
    elif type == 'position':
        res = oanda.get_position(account_id=account_id, instruments=insts_str)
    elif type == 'eco_calendar':
        res = oanda.get_eco_calendar(instruments=insts_str, period=period)
    elif type == 'historical_position_ratios':
        res = oanda.get_historical_position_ratios(
            instruments=insts_str, period=period
        )
    elif type == 'historical_spreads':
        res = oanda.get_historical_spreads(
            instruments=insts_str, period=period
        )
    elif type == 'commitments_of_traders':
        res = oanda.get_commitments_of_traders(instruments=insts_str)
    elif type == 'orderbook':
        res = oanda.get_orderbook(instruments=insts_str, period=period)
    elif type == 'autochartist':
        res = oanda.get_autochartist(instruments=insts_str, period=period)
    logger.debug('Print information: {}'.format(type))
    print(yaml.dump(res, default_flow_style=False))
