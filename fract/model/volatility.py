#!/usr/bin/env python

from datetime import datetime
import logging
import numpy as np
from scipy import stats
from .base import FractTrader, FractTradeHelper


def _calc_hv(array):
    la = np.log(array)
    u = la[1:-1] - la[0:-2]
    return np.std(u, ddof=1)


def _calc_log_diff_ci(array, level=0.95):
    la = np.log(array)
    u = la[1:-1] - la[0:-2]
    return np.asarray(
        stats.t.interval(alpha=level, df=len(u) - 1)
    ) * stats.sem(u) + np.mean(u)


class Volatility(FractTrader):
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
                wi = self._get_window(instrument=instrument)
                ws = self._calc_window_stat(window=wi)
                logging.debug('ws: {}'.format(ws))

                max_spread = ws['std'] * self.model['sigma']['max_spread']
                logging.debug('max_spread: {}'.format(max_spread))

                if prices[instrument]['spread'] > max_spread:
                    helper.print_log('Skip for large spread.')
                else:
                    hv = _calc_hv(
                        array=wi['midpoints'][-self.model['hv']['sample']:]
                    )
                    logging.debug('hv: {}'.format(np.float32(hv)))

                    ld_ci = _calc_log_diff_ci(
                        array=wi['midpoints'][-self.model['ci']['sample']:],
                        level=self.model['ci']['level']
                    )
                    logging.debug('ld_ci: {}'.format(np.float32(ld_ci)))

                    target = wi['midpoints'][-1] * np.exp(
                        np.mean(ld_ci) * self.model['hv']['take_profit']
                    )
                    logging.debug('target: {}'.format(np.float32(target)))

                    if ld_ci[0] > 0 and hv > self.model['hv']['min']:
                            helper.print_order_log(
                                response=self._place_order(sd=ws['std'],
                                                           prices=prices,
                                                           rate=rate,
                                                           side='buy',
                                                           units=units,
                                                           target=target)
                            )
                    elif ld_ci[1] < 0 and hv > self.model['hv']['min']:
                            helper.print_order_log(
                                response=self._place_order(sd=ws['std'],
                                                           prices=prices,
                                                           rate=rate,
                                                           side='sell',
                                                           units=units,
                                                           target=target)
                            )
                    else:
                        helper.print_log('Skip by the criteria.')

        return rate
