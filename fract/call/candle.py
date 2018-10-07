#!/usr/bin/env python

import json
import os
import logging
import sqlite3
import oandapy
import pandas as pd
import pandas.io.sql as pdsql
import yaml
from ..util.config import read_config_yml


def track_rate(config_yml, instruments, granularity, count, sqlite_path=None,
               print_json=False, quiet=False):
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
        sqlite_abspath = os.path.abspath(os.path.expanduser(sqlite_path))
        if os.path.isfile(sqlite_abspath):
            with sqlite3.connect(sqlite_abspath) as con:
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
            with sqlite3.connect(sqlite_abspath) as con:
                con.executescript(sql)
                logger.debug('df:{0}{1}'.format(os.linesep, df))
                pdsql.to_sql(
                    df, 'candle', con, index=False, if_exists='append'
                )
    if not quiet:
        print(
            json.dumps(candles, indent=2) if print_json else
            yaml.dump(candles, default_flow_style=False).strip()
        )
