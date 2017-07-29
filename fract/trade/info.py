#!/usr/bin/env python

import json
import gzip
import os
import logging
import oandapy
from ..cli.util import dump_yaml, FractError


def track_rate(config, instruments, granularity, count, json_path=None):
    oanda = oandapy.API(environment=config['oanda']['environment'],
                        access_token=config['oanda']['access_token'])
    candles = {
        inst: [
            d for d in
            oanda.get_history(account_id=config['oanda']['account_id'],
                              instrument=inst,
                              candleFormat='bidask',
                              granularity=granularity,
                              count=count)['candles']
            if d['complete']
        ]
        for inst
        in (
            instruments if len(instruments) > 0
            else config['trade']['instruments']
        )
    }

    if json_path is None:
        print(json.dumps(candles))
    else:
        ext = json_path.split('.')
        if ext[-1] == 'json':
            if os.path.isfile(json_path):
                with open(json_path, 'r') as f:
                    j = json.load(f)
                with open(json_path, 'w') as f:
                    json.dump(_merge_candles(j, candles), f)
            else:
                with open(json_path, 'w') as f:
                    json.dump(candles, f)
        elif ext[-1] == 'gz' and ext[-2] == 'json':
            if os.path.isfile(json_path):
                with gzip.open(json_path, 'rb') as f:
                    j = json.load(f)
                with gzip.open(json_path, 'wb') as f:
                    f.write(
                        json.dumps(_merge_candles(j, candles)).encode('utf-8')
                    )
            else:
                with gzip.open(json_path, 'wb') as f:
                    f.write(json.dumps(candles).encode('utf-8'))
        else:
            raise FractError('invalid json name')


def _merge_candles(dict_old, dict_new):
    d = dict_new
    for k in dict_old.keys():
        nt = [l['time'] for l in dict_new[k]]
        d[k] = [v for v in dict_old[k] if v['time'] not in nt] + d[k]
    return d


def print_info(config, instruments, type='accounts'):
    oanda = oandapy.API(environment=config['oanda']['environment'],
                        access_token=config['oanda']['access_token'])
    account_id = config['oanda']['account_id']
    cs_instruments = ','.join(instruments)

    if type == 'instruments':
        info = oanda.get_instruments(account_id=account_id)
    elif type == 'prices':
        info = oanda.get_prices(account_id=account_id,
                                instruments=cs_instruments)
    elif type == 'account':
        info = oanda.get_account(account_id=account_id)
    elif type == 'accounts':
        info = oanda.get_accounts()
    elif type == 'orders':
        info = oanda.get_orders(account_id=account_id)
    elif type == 'trades':
        info = oanda.get_trades(account_id=account_id)
    elif type == 'positions':
        info = oanda.get_positions(account_id=account_id)
    elif type == 'position':
        info = oanda.get_position(account_id=account_id,
                                  instruments=cs_instruments)
    elif type == 'transaction':
        info = oanda.get_transaction(account_id=account_id)
    elif type == 'transaction_history':
        info = oanda.get_transaction_history(account_id=account_id)
    elif type == 'eco_calendar':
        info = oanda.get_eco_calendar()
    elif type == 'historical_position_ratios':
        info = oanda.get_historical_position_ratios()
    elif type == 'historical_spreads':
        info = oanda.get_historical_spreads()
    elif type == 'commitments_of_traders':
        info = oanda.get_commitments_of_traders()
    elif type == 'orderbook':
        info = oanda.get_orderbook()
    elif type == 'autochartist':
        info = oanda.get_autochartist()

    logging.debug('Print information: {}'.format(type))
    print(dump_yaml(info))
