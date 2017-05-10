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
        self.account_id = oanda['account_id']
        self.account_currency = self.get_account(
            account_id=self.account_id
        )['accountCurrency']
        self.margin_ratio = margin_ratio
        self.model = model['bollinger']
        self.quiet = quiet
        logging.debug('Bollinger:\n{}'.format(dump_yaml({
            'self.account_id': self.account_id,
            'self.account_currency': self.account_currency,
            'self.margin_ratio': self.margin_ratio,
            'self.model': self.model,
            'self.quiet': self.quiet
        })))
        self.instrument_list = [
            d['instrument'] for d in
            self.get_instruments(account_id=self.account_id)['instruments']
        ]
        logging.debug('self.instrument_list: {}'.format(self.instrument_list))

    def auto(self, instrument):
        time.sleep(0.2)
        rate = self._get_rate(instrument=instrument)
        logging.debug('rate: {}'.format(rate))
        if rate['halted']:
            self._print('Skip for trading halted.', instrument=instrument)
        else:
            time.sleep(0.2)
            price = self._get_price(instrument=rate['instrument'])
            logging.debug('price: {}'.format(price))
            units = self._calc_units(price=price, rate=rate)
            logging.debug('units: {}'.format(units))
            if units == 0:
                self._print('Skip for lack of margin.', instrument=instrument)
            else:
                wi = self._calc_window(instrument=instrument)
                logging.debug('wi: {}'.format(wi))
                max_spread = wi['std'] * self.model['sigma']['max_spread']
                if price['spread'] > max_spread:
                    self._print('Skip for spread.', instrument=instrument)
                elif wi['last'] > wi['up_bound']:
                    od = self._place_order(instrument=instrument,
                                           units=units,
                                           side='buy',
                                           sd=wi['std'],
                                           price=price,
                                           rate=rate)
                    self._print(
                        'Buy {1} units.\n{2}'.format(units, dump_yaml(od)),
                        instrument=instrument
                    )
                elif wi['last'] < wi['low_bound']:
                    od = self._place_order(instrument=instrument,
                                           units=units,
                                           side='sell',
                                           sd=wi['std'],
                                           price=price,
                                           rate=rate)
                    self._print(
                        'Sell {1} units.\n{2}'.format(units, dump_yaml(od)),
                        instrument=instrument
                    )
                else:
                    self._print('Skip by the criteria.', instrument=instrument)
        return rate

    def _get_rate(self, instrument):
        return self.get_instruments(
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

    def _calc_units(self, rate, price):
        cur_pair = rate['instrument'].split('_')
        logging.debug('cur_pair: {}'.format(cur_pair))
        if cur_pair[0] == self.account_currency:
            bp = 1 / price['ask']
        elif cur_pair[1] == self.account_currency:
            bp = price['ask']
        else:
            inst_bp = [
                (inst if inst in self.instrument_list else None)
                for inst in
                map(lambda p: '_'.join(p),
                    [(cur_pair[1], self.account_currency),
                     (self.account_currency, cur_pair[1])])
            ]
            logging.debug('inst_bp: {}'.format(inst_bp))
            if inst_bp[0]:
                bp = price['ask'] * self._get_price(instrument=inst_bp)['ask']
            elif inst_bp[1]:
                bp = price['ask'] / self._get_price(instrument=inst_bp)['ask']
            else:
                raise FractError('invalid instruments')
        logging.debug('bp: {}'.format(bp))

        ac = self.get_account(account_id=self.account_id)
        logging.debug('ac: {}'.format(ac))
        margin_req = (ac['marginAvail'] + ac['marginUsed']) * self.margin_ratio
        logging.debug('margin_req: {}'.format(margin_req))
        unit_margin = bp * rate['marginRate']
        logging.debug('unit_margin: {}'.format(unit_margin))

        if margin_req < ac['marginAvail']:
            units = np.int32(np.floor(margin_req / unit_margin))
            if units <= rate['maxTradeUnits']:
                return units
            else:
                return rate['maxTradeUnits']
        else:
            return 0

    def _calc_window(self, instrument):
        arr = np.array([
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

    def _place_order(self, instrument, units, side, sd, price, rate):
        trail_p = sd * self.model['sigma']['trailing_stop']
        ts = np.int32(np.ceil(
            (trail_p + price['spread']) / np.float32(rate['pip'])
        ))
        if ts > rate['maxTrailingStop']:
            trailing_stop = np.int32(rate['maxTrailingStop'])
        elif ts < rate['minTrailingStop']:
            trailing_stop = np.int32(rate['minTrailingStop'])
        else:
            trailing_stop = ts
        logging.debug('trailing_stop: {}'.format(trailing_stop))

        stop_p = sd * self.model['sigma']['stop_loss']
        if side == 'buy':
            stop_loss = np.float32(price['ask'] - stop_p)
        elif side == 'sell':
            stop_loss = np.float32(price['bid'] + stop_p)
        else:
            raise FractError('invalid side')
        logging.debug('stop_loss: {}'.format(stop_loss))

        return self.create_order(account_id=self.account_id,
                                 units=units,
                                 instrument=instrument,
                                 side=side,
                                 stopLoss=stop_loss,
                                 trailingStop=trailing_stop,
                                 type='market')

    def _get_price(self, instrument):
        p = self.get_prices(account_id=self.account_id,
                            instruments=instrument)['prices'][0]
        return dict(list(p.items()) + [('spread', p['ask'] - p['bid'])])

    def _print(self, message, instrument=None):
        text = '[ {0} - {1} ]\t{2}{3}'.format(
            __package__,
            self.__class__.__name__,
            ((instrument + '\t>>>>>>\t') if instrument else ''),
            message
        )
        if self.quiet:
            logging.debug(text)
        else:
            print(text, flush=True)


def open_deals(config, instruments, n=10, interval=2, quiet=False):
    insts = (instruments if instruments else config['trade']['instruments'])
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    deal = Bollinger(oanda=config['oanda'],
                     model=config['trade']['model'],
                     margin_ratio=config['trade']['margin_ratio'],
                     quiet=quiet)
    deal._print('!!! OPEN DEALS !!!')
    for i in range(n):
        halted = all([
            deal.auto(instrument=inst)['halted']
            for inst in insts
        ])
        if halted or i == n - 1:
            break
        else:
            time.sleep(interval)
