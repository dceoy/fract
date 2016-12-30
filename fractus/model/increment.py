#!/usr/bin/env python

import logging
import numpy as np
import signal
import time
import oandapy
from ..cli.yaml import print_as_yaml


class Increment(oandapy.API):
    RATES = {'buy': 'ask', 'sell': 'bid'}

    def __init__(self, account_id, instrument, units, stops, **kwargs):
        super().__init__(**kwargs)
        self.account_id = account_id
        self.instrument = instrument
        self.open_unit = units['opening']
        self.unit = self.open_unit
        self.plus_unit = units['increment']
        self.stop = stops['loss']
        self.trail = stops['trail']

        self.side = np.random.choice(tuple(self.RATES.keys()))
        logging.debug('opening side: {}'.format(self.side))

        info = self.get_instruments(account_id=account_id,
                                    instruments=self.instrument)
        logging.debug('instrument:\n{}'.format(info))
        self.pip = float(info['instruments'][0]['pip'])
        self.max_units = info['instruments'][0]['maxTradeUnits']

    def deal(self):
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        opened = self._create_market_order(units=self.unit,
                                           side=self.side)
        logging.debug('opening:\n{}'.format(opened))
        print_as_yaml(opened)
        trade_id = opened['tradeOpened']['id']

        while True:
            time.sleep(5)
            transaction = self._get_transaction(transaction_id=trade_id)
            logging.debug('transaction:\n{}'.format(transaction))
            print_as_yaml(transaction)

    def _create_market_order(self, units, side):
        price = self.get_prices(instruments=self.instrument)
        logging.debug('current price:\n{}'.format(price))

        anchor = price['prices'][0][self.RATES[self.side]]
        logging.debug('anchor price: {}'.format(anchor))
        stop_prop = {'buy': 1 - self.stop, 'sell': 1 + self.stop}[self.side]
        stopl = {'loss': int(anchor * stop_prop / self.pip) * self.pip,
                 'trail': int(anchor * self.trail / self.pip)}
        logging.debug('stop prices: {}'.format(stopl))

        return self.create_order(self.account_id,
                                 instrument=self.instrument,
                                 units=units,
                                 side=side,
                                 stopLoss=stopl['loss'],
                                 trailingStop=stopl['trail'],
                                 type='market')

    def _get_transaction(self, transaction_id):
        return self.get_transaction(self.account_id,
                                    transaction_id=transaction_id)

    def _get_trade(self, trade_id):
        return self.get_trade(self.account_id,
                              instrument=self.instrument,
                              trade_id=trade_id)


def auto(config):
    ai = Increment(environment=config['oanda']['environment'],
                   access_token=config['oanda']['access_token'],
                   account_id=config['oanda']['account_id'],
                   instrument=config['oanda']['currency_pair'][0],
                   units={'opening': 1, 'increment': 1},
                   stops={'loss': 0.01, 'trail': 0.02})
    ai.deal()
