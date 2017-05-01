#!/usr/bin/env python

import logging
import oandapy
from ..cli.yaml import print_as_yaml


def print_info(config, type='accounts', instruments=['EUR_USD']):
    oanda = oandapy.API(environment=config['oanda']['environment'],
                        access_token=config['oanda']['access_token'])
    account_id = config['oanda']['account_id']
    cs_instruments = ','.join(instruments)

    if type == 'instruments':
        info = oanda.get_instruments(account_id=account_id)
    elif type == 'prices':
        info = oanda.get_prices(account_id=account_id,
                                instruments=cs_instruments)
    elif type == 'history':
        info = oanda.get_history(account_id=account_id,
                                 instrument=instruments[0])
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
    print_as_yaml(info)
