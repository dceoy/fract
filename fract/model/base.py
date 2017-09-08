#!/usr/bin/env python

from datetime import datetime
import logging
import os
import time
import numpy as np
import oandapy
from ..cli.util import dump_yaml, FractError


class FractTrader(oandapy.API):
    def __init__(self, oanda, margin_ratio, model, quiet=False):
        super().__init__(environment=oanda['environment'],
                         access_token=oanda['access_token'])
        self.account_id = oanda['account_id']
        self.account_currency = self.get_account(
            account_id=self.account_id
        )['accountCurrency']
        self.margin_ratio = margin_ratio
        self.model = model
        self.quiet = quiet
        logging.debug(
            '{0}:{1}{2}'.format(
                self.__class__.__name__,
                os.linesep,
                dump_yaml({'self.account_id': self.account_id,
                           'self.account_currency': self.account_currency,
                           'self.margin_ratio': self.margin_ratio,
                           'self.model': self.model,
                           'self.quiet': self.quiet})
            )
        )
        self.instrument_list = [
            d['instrument'] for d in
            self.get_instruments(account_id=self.account_id)['instruments']
        ]
        logging.debug('self.instrument_list: {}'.format(self.instrument_list))

    def _get_prices(self):
        return dict(
            map(lambda p:
                (p['instrument'],
                 {'bid': p['bid'],
                  'ask': p['ask'],
                  'spread': np.float32(p['ask'] - p['bid'])}),
                self.get_prices(
                    account_id=self.account_id,
                    instruments=','.join(self.instrument_list)
                )['prices'])
        )

    def _get_margin(self):
        return (
            lambda a:
            {'avail': a['marginAvail'],
             'used': a['marginUsed'],
             'total': a['marginAvail'] + a['marginUsed']}
        )(self.get_account(account_id=self.account_id))

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

    def _get_window(self, instrument):
        return {
            'instrument': instrument,
            'midpoints': np.array([
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
        }

    def _calc_units(self, rate, prices, margin):
        inst = rate['instrument']
        cur_pair = inst.split('_')
        logging.debug('cur_pair: {}'.format(cur_pair))
        if cur_pair[0] == self.account_currency:
            bp = 1 / prices[inst]['ask']
        elif cur_pair[1] == self.account_currency:
            bp = prices[inst]['ask']
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
                bp = prices[inst]['ask'] * prices[inst_bp[0]]['ask']
            elif inst_bp[1]:
                bp = prices[inst]['ask'] / prices[inst_bp[1]]['ask']
            else:
                raise FractError('invalid instruments')
        logging.debug('bp: {}'.format(bp))

        mg = dict([(k, v * (margin['avail'] + margin['used']))
                   for k, v in self.margin_ratio.items()])
        logging.debug('mg: {}'.format(mg))
        mg_per_unit = bp * rate['marginRate']
        logging.debug('mg_per_unit: {}'.format(mg_per_unit))

        if mg['ticket'] < (margin['avail'] - mg['preserve']):
            units = np.int32(np.floor(mg['ticket'] / mg_per_unit))
            if units <= rate['maxTradeUnits']:
                return units
            else:
                return rate['maxTradeUnits']
        else:
            return 0

    def _calc_window_stat(self, window):
        if window['midpoints'].shape[0] == self.model['window']['size']:
            return (
                lambda i, f, l, m, s, v:
                {'instrument': i,
                 'first': np.float32(f),
                 'last': np.float32(l),
                 'mean': np.float32(m),
                 'std': np.float32(s),
                 'var': np.float32(v)}
            )(
                i=window['instrument'],
                f=window['midpoints'][0],
                l=window['midpoints'][-1],
                m=window['midpoints'].mean(),
                s=window['midpoints'].std(ddof=1),
                v=window['midpoints'].var(ddof=1)
            )
        else:
            raise FractError('window size not matched')

    def _place_order(self, prices, rate, side, units, ld=None, sd=None):
        pr = prices[rate['instrument']]
        if side in {'buy', 'sell'}:
            spr = {'buy': pr['ask'], 'sell': pr['bid']}[side]
            if sd is not None:
                signed_sd = {'buy': sd, 'sell': - sd}[side]
            elif ld is None:
                raise FractError('ld or sd required')
        else:
            raise FractError('invalid side')

        ts = np.int16(np.ceil(
            (
                sd * self.model['sigma']['trailing_stop'] + pr['spread']
                if ld is None else
                spr * abs(np.exp(ld * self.model['hv']['trailing_stop']) - 1)
            ) / np.float32(rate['pip'])
        ))
        if ts > rate['maxTrailingStop']:
            trailing_stop = np.int16(rate['maxTrailingStop'])
        elif ts < rate['minTrailingStop']:
            trailing_stop = np.int16(rate['minTrailingStop'])
        else:
            trailing_stop = ts
        logging.debug('trailing_stop: {}'.format(trailing_stop))

        stop_loss = np.float16(
            spr - signed_sd * self.model['sigma']['stop_loss']
            if ld is None else
            spr * np.exp(- ld * self.model['hv']['stop_loss'])
        )
        logging.debug('stop_loss: {}'.format(stop_loss))

        take_profit = np.float16(
            spr + signed_sd * self.model['sigma']['take_profit']
            if ld is None else
            spr * np.exp(ld * self.model['hv']['take_profit'])
        )
        logging.debug('take_profit: {}'.format(take_profit))

        return self.create_order(account_id=self.account_id,
                                 units=units,
                                 instrument=rate['instrument'],
                                 side=side,
                                 takeProfit=take_profit,
                                 stopLoss=stop_loss,
                                 trailingStop=trailing_stop,
                                 type='market')


class FractTradeHelper(object):
    def __init__(self, instrument, name, quiet):
        self.instrument = instrument
        self.name = name
        self.quiet = quiet

    def print_log(self, message):
        text = '[ {0} - {1}{2}]\t{3}\t>>>>>>\t{4}'.format(
            __package__,
            self.name,
            (lambda l: ' ' * (10 - l) if l < 10 else ' ')(l=len(self.name)),
            self.instrument,
            message
        )
        if self.quiet:
            logging.debug(text)
        else:
            print(text, flush=True)

    def print_order_log(self, response):
        self.print_log(
            '{0} {1} units.{2}{3}'.format(
                response['tradeOpened']['side'].capitalize(),
                response['tradeOpened']['units'],
                os.linesep,
                dump_yaml(response)
            )
        )

    def sleep(self, last, sec=0.5):
        rest = sec - (datetime.now() - last).total_seconds()
        if rest > 0:
            time.sleep(rest)
