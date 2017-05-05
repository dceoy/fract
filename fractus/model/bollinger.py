#!/usr/bin/env python

import logging
import numpy as np
import oandapy
from ..cli.util import dump_yaml, FractError


class Bollinger(oandapy.API):
    def __init__(self, config):
        super().__init__(environment=config['oanda']['environment'],
                         access_token=config['oanda']['access_token'])
        self.account_id = config['oanda']['account_id']
        self.instrument = config['trade']['instrument']
        self.margin_ratio = config['trade']['margin_ratio']
        self.model = config['trade']['model']['bollinger']

    def open(self):
        st = self._fetch_state()
        if st['instrument']['halted']:
            logging.debug('{} is halted.'.format(self.instrument))
        else:
            units = self._calc_units(state=st)
            if units == 0:
                logging.debug('Skip ordering for margin.')
            else:
                wo = self._calc_window_orders(state=st)
                if wo['last'] > wo['upper_band']:
                    long = self._create_order(units=units,
                                              side='buy',
                                              stopLoss=wo['lower_stop'],
                                              trailingStop=wo['trail_pip'])
                    logging.debug('long:\n{}'.format(long))
                elif wo['last'] < wo['lower_band']:
                    short = self._create_order(units=units,
                                               side='short',
                                               stopLoss=wo['upper_stop'],
                                               trailingStop=wo['trail_pip'])
                    logging.debug('short:\n{}'.format(short))
                else:
                    logging.debug('Skip ordering by the criteria.')

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
                fields='%2C'.join(['displayName',
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
        base_cur = self.instrument.split('_')[1]
        if base_cur == 'JPY':
            price_j = price
        else:
            inst_j = list(filter(lambda c:
                                 set(c.split('_')) == {base_cur, 'JPY'},
                                 self._fetch_instrument_list()))[0]
            if inst_j == (base_cur + '_JPY'):
                price_j = price * self._fetch_ask_price(instrument=inst_j)
            elif inst_j == ('JPY_' + base_cur):
                price_j = price / self._fetch_ask_price(instrument=inst_j)
            else:
                raise FractError('invalid instruments')
        unit_margin = price_j * state['instrument']['marginRate']
        margin_avail = state['account']['marginAvail']
        margin_required = (margin_avail +
                           state['account']['marginUsed']) * self.margin_ratio
        if margin_required < margin_avail:
            return np.floor(margin_required / unit_margin)
        else:
            return 0

    def _calc_window_orders(self, state):
        arr = self._fetch_window()
        l = arr[-1]
        m = arr.mean()
        s = arr.std()
        os = self.model['order_sigma']
        return {
            'last': l,
            'mean': m,
            'std': s,
            'upper_band': m + s * os['entry'],
            'lower_band': m - s * os['entry'],
            'upper_stop': l + s * os['stop_loss'],
            'lower_stop': l - s * os['stop_loss'],
            'trail_pip': s * os['trailing_stop'] / state['instrument']['pip']
        }

    def _create_order(self, **kwargs):
        self.create_order(account_id=self.account_id,
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
                granularity=self.model['band']['granularity'],
                count=self.model['band']['windows']
            )['candles']
        ])


def invoke(config):
    deal = Bollinger(config=config)
    logging.debug('deal:\n{}'.format(dump_yaml(vars(deal))))
    deal.open()
