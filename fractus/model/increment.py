#!/usr/bin/env python

import logging
import numpy as np
import signal
import time
import oandapy
from ..cli.yaml import print_as_yaml


class Increment(oandapy.API):
    RATES = {'buy': 'ask', 'sell': 'bid'}

    def __init__(self, account_id, instrument, param, **kwargs):
        super().__init__(**kwargs)

        self.account_id = account_id
        self.instrument = instrument
        self.unit_open = param['unit']['opening']
        self.unit = self.unit_open
        self.unit_plus = param['unit']['increment']
        self.stop = param['stop']['loss']
        self.trail = param['stop']['trail']
        self.interval = param['interval']
        self.side = np.random.choice(tuple(self.RATES.keys()))
        self.index = 0

        info = self.get_instruments(account_id=account_id,
                                    instruments=self.instrument)
        logging.debug('instrument:\n{}'.format(info))
        self.pip = float(info['instruments'][0]['pip'])
        self.unit_max = info['instruments'][0]['maxTradeUnits']

    def deal(self):
        print('model: Increment\n\n<<<<< OPEN DEALS >>>>>')
        while True:
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            self.index += 1
            print('\n>>> DEAL #{}'.format(self.index))

            pr = self.get_prices(instruments=self.instrument)
            logging.debug('current price:\n{}'.format(pr))
            sl = self._calc_stop(anchor=pr['prices'][0][self.RATES[self.side]])

            opened = self.create_order(self.account_id,
                                       instrument=self.instrument,
                                       units=self.unit,
                                       side=self.side,
                                       stopLoss=sl['loss'],
                                       trailingStop=sl['trail'],
                                       type='market')
            logging.debug('opening:\n{}'.format(opened))
            print_as_yaml({'opening': opened})

            trade_id = opened['tradeOpened']['id']
            trade_is_open = True

            while trade_is_open:
                time.sleep(self.interval)

                trs = self.get_transaction_history(account_id=self.account_id,
                                                   instrument=self.instrument,
                                                   minId=trade_id)
                logging.debug('transactions:\n{}'.format(trs))
                tr = list(filter(lambda t:
                                 'tradeId' in t and
                                 t['tradeId'] == trade_id,
                                 trs['transactions']))

                if len(tr) == 0:
                    logging.debug('continue')
                else:
                    logging.debug('closing:\n{}'.format(tr))
                    print_as_yaml({'closing': tr[0]})
                    tr_type = tr[0]['type']
                    logging.debug('transaction type: {}'.format(tr_type))

                    if tr_type in ('STOP_LOSS_FILLED', 'TRAILING_STOP_FILLED'):
                        if self._profit_taken(open=opened['price'],
                                              close=tr['price'],
                                              side=self.side):
                            self.unit = self.unit_open
                        else:
                            self.unit += self.unit_plus
                            self.side = self._reverse_side(side=self.side)
                    else:
                        self.unit = self.unit_open

                    trade_is_open = False
                    logging.debug('break')

    def _calc_stop(self, anchor):
        prop = {'buy': 1 - self.stop, 'sell': 1 + self.stop}
        return {
            'loss': int(anchor * prop[self.side] / self.pip) * self.pip,
            'trail': int(anchor * self.trail / self.pip)
        }

    def _profit_taken(open, close, side):
        if side == 'buy':
            if open < close:
                return True
            else:
                return False
        elif side == 'sell':
            if open > close:
                return True
            else:
                return False

    def _reverse_side(side):
        if side == 'buy':
            return 'sell'
        elif side == 'sell':
            return 'buy'


def open_deal(config, instrument):
    ai = Increment(environment=config['oanda']['environment'],
                   access_token=config['oanda']['access_token'],
                   account_id=config['oanda']['account_id'],
                   instrument=instrument,
                   param=config['model']['increment'])
    ai.deal()
