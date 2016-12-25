#!/usr/bin/env python

import logging
import yaml
import oandapy


def print_account(config, list_accounts=False):
    oanda = oandapy.API(environment=config['oanda']['environment'],
                        access_token=config['oanda']['access_token'])
    if list_accounts:
        logging.debug('Print a list of accounts')
        info = oanda.get_accounts()
    else:
        logging.debug('Print account\'s infomation')
        info = oanda.get_account(account_id=config['oanda']['account_id'])

    print(yaml.dump(info, default_flow_style=False))
