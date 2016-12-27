#!/usr/bin/env python

import logging
import signal
import numpy as np
import oandapy


class Twice(oandapy.API):
    def __init__(self, account_id, instrument, **kwargs):
        super().__init__(**kwargs)
        self.account_id = account_id
        self.instrument = instrument
        self.side = np.random.choice(('buy', 'sell'))
        logging.debug('Set the opening randomly: {}'.format(self.side))

    def auto(self):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        self.create_order(self.account_id,
                          instrument=self.instrument,
                          units=1,
                          side=self.side,
                          type='market')


def play(config):
    trader = Twice(environment=config['oanda']['environment'],
                   access_token=config['oanda']['access_token'],
                   account_id=config['oanda']['account_id'],
                   instrument=config['oanda']['currency_pair']['trade'])
    trader.auto()
