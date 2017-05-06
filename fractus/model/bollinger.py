#!/usr/bin/env python

import logging
import numpy as np
import signal
import time
import oandapy
from ..cli.util import dump_yaml, FractError


class Bollinger(oandapy.API):
    def __init__(self, config, quiet=False):
        super().__init__(environment=config['oanda']['environment'],
                         access_token=config['oanda']['access_token'])
        self.quiet = quiet
        self.account_id = config['oanda']['account_id']
        self.instrument = config['trade']['instrument']
        self.margin_ratio = config['trade']['margin_ratio']
        self.model = config['trade']['model']['bollinger']
        logging.debug('Bollinger:\n{}'.format(dump_yaml({
            'self.quiet': self.quiet,
            'self.account_id': self.account_id,
            'self.instrument': self.instrument,
            'self.margin_ratio': self.margin_ratio,
            'self.model': self.model
        })))

    def order(self):
        st = self._fetch_state()
        logging.debug('st:\n{}'.format(dump_yaml(st)))
        if st['instrument']['halted']:
            self._print('Trading of {} is halted.'.format(self.instrument))
        else:
            units = self._calc_units(state=st)
            logging.debug('units: {}'.format(units))
            if units == 0:
                self._print('Skip ordering for lack of margin.')
            else:
                wi = self._calc_window()
                logging.debug('wi: {}'.format(wi))
                if wi['last'] >= wi['upper_bound']:
                    logging.debug('current({0}) >= upper({1})'.format(
                        wi['last'], wi['upper_bound']
                    ))
                    sl = self._calc_stop_loss(window=wi, state=st)
                    logging.debug('sl: {}'.format(sl))
                    long = self._create_order(units=units,
                                              side='buy',
                                              stopLoss=sl['lower_stop'],
                                              trailingStop=sl['trailing_pip'])
                    self._print('Buy {0} units of {1}.\n{2}'.format(
                        units, self.instrument, dump_yaml(long)
                    ))
                elif wi['last'] <= wi['lower_bound']:
                    logging.debug('current({0}) <= lower({1})'.format(
                        wi['last'], wi['lower_bound']
                    ))
                    sl = self._calc_stop_loss(window=wi, state=st)
                    logging.debug('sl: {}'.format(sl))
                    short = self._create_order(units=units,
                                               side='sell',
                                               stopLoss=sl['upper_stop'],
                                               trailingStop=sl['trailing_pip'])
                    self._print('Sell {0} units of {1}.\n{2}'.format(
                        units, self.instrument, dump_yaml(short)
                    ))
                else:
                    logging.debug(
                        'lower ({0}) < current ({1}) < upper ({2})'.format(
                            wi['lower_bound'], wi['last'], wi['upper_bound']
                        )
                    )
                    self._print('Skip ordering for {} by the criteria.'.format(
                        self.instrument
                    ))
        return st

    def _fetch_state(self):
        return {
            'account':
            self.get_account(
                account_id=self.account_id
            ),
            'instrument':
            self.get_instruments(
                account_id=self.account_id,
                instruments=self.instrument,
                fields=','.join(['displayName',
                                 'pip',
                                 'maxTradeUnits',
                                 'precision',
                                 'maxTrailingStop',
                                 'minTrailingStop',
                                 'marginRate',
                                 'halted'])
            )['instruments'][0]
        }

    def _calc_units(self, state):
        price = self._fetch_ask_price(instrument=self.instrument)
        logging.debug('price: {}'.format(price))

        acc_cur = state['account']['accountCurrency']
        cur_pair = self.instrument.split('_')
        logging.debug('cur_pair: {}'.format(cur_pair))
        if cur_pair[0] == acc_cur:
            price_j = 1 / price
        elif cur_pair[1] == acc_cur:
            price_j = price
        else:
            il = self._fetch_instrument_list()
            inst_j = [(p if p in il else None) for p in
                      [(cur_pair[1], acc_cur), (acc_cur, cur_pair[1])]]
            logging.debug('inst_j: {}'.format(inst_j))
            if inst_j[0]:
                price_j = price * self._fetch_ask_price(instrument=inst_j)
            elif inst_j[1]:
                price_j = price / self._fetch_ask_price(instrument=inst_j)
            else:
                raise FractError('invalid instruments')
        logging.debug('price_j: {}'.format(price))

        unit_margin = price_j * state['instrument']['marginRate']
        logging.debug('unit_margin: {}'.format(unit_margin))
        margin_avail = state['account']['marginAvail']
        logging.debug('margin_avail: {}'.format(margin_avail))
        margin_required = (margin_avail +
                           state['account']['marginUsed']) * self.margin_ratio
        logging.debug('margin_required: {}'.format(margin_required))

        if margin_required < margin_avail:
            units = np.int32(np.floor(margin_required / unit_margin))
            if units <= state['instrument']['maxTradeUnits']:
                return units
            else:
                return state['instrument']['maxTradeUnits']
        else:
            return 0

    def _calc_window(self):
        arr = self._fetch_window()
        logging.debug('arr.shape: {}'.format(arr.shape))
        m = arr.mean()
        s = arr.std()
        return {
            'last': np.float32(arr[-1]),
            'mean': np.float32(m),
            'std': np.float32(s),
            'upper_bound': np.float32(m + s * self.model['sigma']['entry']),
            'lower_bound': np.float32(m - s * self.model['sigma']['entry'])
        }

    def _calc_stop_loss(self, window, state):
        n_sigma = window['std'] * self.model['sigma']['stop_loss']
        tp = np.ceil(window['std'] *
                     self.model['sigma']['trailing_stop'] /
                     np.float32(state['instrument']['pip']))
        if tp > state['instrument']['maxTrailingStop']:
            trailing_pip = state['instrument']['maxTrailingStop']
        elif tp < state['instrument']['minTrailingStop']:
            trailing_pip = state['instrument']['minTrailingStop']
        else:
            trailing_pip = tp
        return {
            'upper_stop': np.float32(window['last'] + n_sigma),
            'lower_stop': np.float32(window['last'] - n_sigma),
            'trailing_pip': np.int32(trailing_pip)
        }

    def _create_order(self, **kwargs):
        return self.create_order(account_id=self.account_id,
                                 instrument=self.instrument,
                                 type='market',
                                 **kwargs)

    def _fetch_instrument_list(self):
        return [
            d['instrument'] for d in
            self.get_instruments(account_id=self.account_id)['instruments']
        ]

    def _fetch_ask_price(self, instrument):
        return self.get_prices(account_id=self.account_id,
                               instruments=instrument)['prices'][0]['ask']

    def _fetch_window(self):
        return np.array([
            d['closeMid']
            for d in
            self.get_history(
                account_id=self.account_id,
                candleFormat='midpoint',
                instrument=self.instrument,
                granularity=self.model['window']['granularity'],
                count=self.model['window']['size']
            )['candles']
        ])

    def _print(self, message):
        text = '[ {0} - {1} ]\t>>>>>>\t{2}'.format(
            __package__,
            self.__class__.__name__,
            message
        )
        logging.debug(text) if self.quiet else print(text)


def open_deals(config, n=10, interval=2, quiet=False):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    deal = Bollinger(config=config, quiet=quiet)
    deal._print('!!! OPEN DEALS !!!')
    for i in range(n):
        st = deal.order()
        if st['instrument']['halted'] or i == n - 1:
            break
        else:
            time.sleep(interval)
