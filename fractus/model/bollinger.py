#!/usr/bin/env python

import logging
import signal
import time
import numpy as np
import oandapy
from ..cli.util import dump_yaml, FractError


class Bollinger(oandapy.API):
    def __init__(self, oanda, margin_ratio, model, quiet=False):
        super().__init__(environment=oanda['environment'],
                         access_token=oanda['access_token'])
        self.quiet = quiet
        self.account_id = oanda['account_id']
        self.margin_ratio = margin_ratio
        self.model = model['bollinger']
        logging.debug('Bollinger:\n{}'.format(dump_yaml({
            'self.quiet': self.quiet,
            'self.account_id': self.account_id,
            'self.margin_ratio': self.margin_ratio,
            'self.model': self.model
        })))

    def auto(self, instrument):
        st = self._fetch_state(instrument=instrument)
        logging.debug('st: {}'.format(st))
        if st['rate']['halted']:
            self._print(
                '{}: Skip ordering for trading halted.'.format(instrument)
            )
        else:
            units = self._calc_units(state=st)
            logging.debug('units: {}'.format(units))
            if units == 0:
                self._print(
                    '{}: Skip ordering for lack of margin.'.format(instrument)
                )
            else:
                wi = self._calc_window(instrument=instrument)
                logging.debug('wi: {}'.format(wi))
                if wi['last'] >= wi['up_bound']:
                    logging.debug('current({0}) >= upper({1})'.format(
                        wi['last'], wi['up_bound']
                    ))
                    long = self._place_order(instrument=instrument,
                                             units=units,
                                             side='buy',
                                             sd=wi['std'],
                                             state_rate=st['rate'])
                    self._print(
                        '{0}: Buy {1} units.\n{2}'.format(
                            instrument, units, dump_yaml(long)
                        )
                    )
                elif wi['last'] <= wi['low_bound']:
                    logging.debug('current({0}) <= lower({1})'.format(
                        wi['last'], wi['low_bound']
                    ))
                    short = self._place_order(instrument=instrument,
                                              units=units,
                                              side='sell',
                                              sd=wi['std'],
                                              state_rate=st['rate'])
                    self._print(
                        '{0}: Sell {1} units.\n{2}'.format(
                            instrument, units, dump_yaml(short)
                        )
                    )
                else:
                    logging.debug(
                        '{0}: low ({1}) < current ({2}) < up ({3})'.format(
                            instrument,
                            wi['low_bound'], wi['last'], wi['up_bound']
                        )
                    )
                    self._print(
                        '{}: Skip ordering by the criteria.'.format(instrument)
                    )
        return st

    def _fetch_state(self, instrument):
        return {
            'account':
            self.get_account(
                account_id=self.account_id
            ),
            'rate':
            self.get_instruments(
                account_id=self.account_id,
                instruments=instrument,
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
        ask = self._fetch_price(instrument=state['rate']['instrument'])['ask']
        logging.debug('ask: {}'.format(ask))

        acc_cur = state['account']['accountCurrency']
        cur_pair = state['rate']['instrument'].split('_')
        logging.debug('cur_pair: {}'.format(cur_pair))
        if cur_pair[0] == acc_cur:
            price_bp = 1 / ask
        elif cur_pair[1] == acc_cur:
            price_bp = ask
        else:
            inst_list = self._fetch_instrument_list()
            inst_bp = [
                (inst if inst in inst_list else None)
                for inst in
                map(lambda p: '_'.join(p),
                    [(cur_pair[1], acc_cur), (acc_cur, cur_pair[1])])
            ]
            logging.debug('inst_bp: {}'.format(inst_bp))
            if inst_bp[0]:
                price_bp = ask * self._fetch_price(instrument=inst_bp)['ask']
            elif inst_bp[1]:
                price_bp = ask / self._fetch_price(instrument=inst_bp)['ask']
            else:
                raise FractError('invalid instruments')
        logging.debug('price_bp: {}'.format(price_bp))

        unit_margin = price_bp * state['rate']['marginRate']
        logging.debug('unit_margin: {}'.format(unit_margin))
        margin_avail = state['account']['marginAvail']
        logging.debug('margin_avail: {}'.format(margin_avail))
        margin_required = (margin_avail +
                           state['account']['marginUsed']) * self.margin_ratio
        logging.debug('margin_required: {}'.format(margin_required))

        if margin_required < margin_avail:
            units = np.int32(np.floor(margin_required / unit_margin))
            if units <= state['rate']['maxTradeUnits']:
                return units
            else:
                return state['rate']['maxTradeUnits']
        else:
            return 0

    def _calc_window(self, instrument):
        arr = self._fetch_window(instrument=instrument)
        logging.debug('arr.shape: {}'.format(arr.shape))
        m = arr.mean()
        s = arr.std()
        return {
            'instrument': instrument,
            'last': np.float32(arr[-1]),
            'mean': np.float32(m),
            'std': np.float32(s),
            'up_bound': np.float32(m + s * self.model['sigma']['entry']),
            'low_bound': np.float32(m - s * self.model['sigma']['entry'])
        }

    def _place_order(self, instrument, units, side, sd, state_rate):
        p = self._fetch_price(instrument=instrument)
        ts = np.int32(np.ceil(
            (sd * self.model['sigma']['trailing_stop'] + p['ask'] - p['bid']) /
            np.float32(state_rate['pip'])
        ))
        if ts > state_rate['maxTrailingStop']:
            trailing_stop = np.int32(state_rate['maxTrailingStop'])
        elif ts < state_rate['minTrailingStop']:
            trailing_stop = np.int32(state_rate['minTrailingStop'])
        else:
            trailing_stop = ts
        logging.debug('trailing_stop: {}'.format(trailing_stop))
        if side == 'buy':
            stop = np.float32(p['bid'] - sd * self.model['sigma']['stop_loss'])
        elif side == 'sell':
            stop = np.float32(p['ask'] + sd * self.model['sigma']['stop_loss'])
        else:
            raise FractError('invalid side')
        logging.debug('stop: {}'.format(stop))
        return self.create_order(account_id=self.account_id,
                                 units=units,
                                 instrument=instrument,
                                 side=side,
                                 stopLoss=stop,
                                 trailingStop=trailing_stop,
                                 type='market')

    def _fetch_instrument_list(self):
        return [
            d['instrument'] for d in
            self.get_instruments(account_id=self.account_id)['instruments']
        ]

    def _fetch_price(self, instrument):
        return self.get_prices(account_id=self.account_id,
                               instruments=instrument)['prices'][0]

    def _fetch_window(self, instrument):
        return np.array([
            d['closeMid']
            for d in
            self.get_history(
                account_id=self.account_id,
                candleFormat='midpoint',
                instrument=instrument,
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
        if self.quiet:
            logging.debug(text)
        else:
            print(text, flush=True)


def open_deals(config, instruments, n=10, interval=2, quiet=False):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    deal = Bollinger(oanda=config['oanda'],
                     model=config['trade']['model'],
                     margin_ratio=config['trade']['margin_ratio'],
                     quiet=quiet)
    deal._print('!!! OPEN DEALS !!!')
    for i in range(n):
        halted = all([
            deal.auto(instrument=inst)['rate']['halted']
            for inst in instruments
        ])
        if halted or i == n - 1:
            break
        else:
            time.sleep(interval)
