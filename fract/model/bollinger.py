#!/usr/bin/env python

from datetime import datetime
import logging
from .base import FractTrader, FractTradeHelper


class Bollinger(FractTrader):
    def __init__(self, oanda, margin_ratio, model, quiet=False):
        super().__init__(oanda=oanda,
                         margin_ratio=margin_ratio,
                         model=model,
                         quiet=quiet)

    def fire(self, instrument):
        t0 = datetime.now()
        rate = self._get_rate(instrument=instrument)
        logging.debug('rate: {}'.format(rate))
        helper = FractTradeHelper(name=self.__class__.__name__,
                                  instrument=instrument,
                                  quiet=self.quiet)

        if rate['halted']:
            helper.print_log('Skip for trading halted.')
            helper.sleep(last=t0, sec=0.5)
        else:
            prices = self._get_prices()
            logging.debug('prices: {}'.format(prices))
            helper.sleep(last=t0, sec=0.5)

            units = self._calc_units(rate=rate,
                                     prices=prices,
                                     margin=self._get_margin())
            logging.debug('units: {}'.format(units))
            helper.sleep(last=t0, sec=1)

            if units == 0:
                helper.print_log('Skip for lack of margin.')
            else:
                ws = self._calc_window_stat(
                    window=self._get_window(instrument=instrument)
                )
                logging.debug('ws: {}'.format(ws))

                max_spread = ws['std'] * self.model['sigma']['max_spread']
                logging.debug('max_spread: {}'.format(max_spread))

                if prices[instrument]['spread'] > max_spread:
                    helper.print_log('Skip for large spread.')
                elif ws['last'] > ws['up_bound']:
                    helper.print_order_log(
                        response=self._place_order(sd=ws['std'],
                                                   prices=prices,
                                                   rate=rate,
                                                   side='buy',
                                                   units=units)
                    )
                elif ws['last'] < ws['low_bound']:
                    helper.print_order_log(
                        response=self._place_order(sd=ws['std'],
                                                   prices=prices,
                                                   rate=rate,
                                                   side='sell',
                                                   units=units)
                    )
                else:
                    helper.print_log('Skip by the criteria.')

        return rate
