#!/usr/bin/env python

import logging
import yaml
import oandapy


def fetch_info(config, type):
    oanda = oandapy.API(environment=config['oanda']['environment'],
                        access_token=config['oanda']['access_token'])
    if type == 'instruments':
        return oanda.get_instruments(account_id=config['oanda']['account_id'])
    elif type == 'prices':
        return oanda.get_prices(account_id=config['oanda']['account_id'])
    elif type == 'history':
        return oanda.get_history(account_id=config['oanda']['account_id'])
    elif type == 'account':
        return oanda.get_account(account_id=config['oanda']['account_id'])
    elif type == 'accounts':
        return oanda.get_accounts()
    elif type == 'orders':
        return oanda.get_orders(account_id=config['oanda']['account_id'])
    elif type == 'trades':
        return oanda.get_trades(account_id=config['oanda']['account_id'])
    elif type == 'positions':
        return oanda.get_positions(account_id=config['oanda']['account_id'])
    elif type == 'position':
        return oanda.get_position(account_id=config['oanda']['account_id'],
                                  instrument=config['oanda']['currency_pair']['trade'])
    elif type == 'transaction':
        return oanda.get_transaction(account_id=config['oanda']['account_id'])
    elif type == 'transaction_history':
        return oanda.get_transaction_history(account_id=config['oanda']['account_id'])
    elif type == 'eco_calendar':
        return oanda.get_eco_calendar()
    elif type == 'historical_position_ratios':
        return oanda.get_historical_position_ratios()
    elif type == 'historical_spreads':
        return oanda.get_historical_spreads()
    elif type == 'commitments_of_traders':
        return oanda.get_commitments_of_traders()
    elif type == 'orderbook':
        return oanda.get_orderbook()
    elif type == 'autochartist':
        return oanda.get_autochartist()


def print_info(config, type='accounts'):
    logging.debug('Print information about {}'.format(type))
    print(yaml.dump(fetch_info(config, type), default_flow_style=False))
