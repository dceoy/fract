#!/usr/bin/env python

import json
import logging
import oandapy
import yaml
from ..util.error import FractRuntimeError
from ..util.config import read_config_yml


def print_info(config_yml, instruments=[], type='accounts', print_json=False):
    logger = logging.getLogger(__name__)
    available_types = [
        'instruments', 'account', 'accounts', 'orders', 'trades', 'positions',
        'transactions', 'prices', 'position', 'eco_calendar',
        'historical_position_ratios', 'historical_spreads',
        'commitments_of_traders', 'orderbook', 'autochartist',
    ]
    if type not in available_types:
        raise FractRuntimeError('invalid info type: {}'.format(type))
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
        raise FractRuntimeError('{}: instruments required'.format(type))
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
    print(
        json.dumps(res, indent=2) if print_json else
        yaml.dump(res, default_flow_style=False).strip()
    )
