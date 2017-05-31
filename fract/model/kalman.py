#!/usr/bin/env python

from datetime import datetime
import logging
import numpy as np
from .base import FractTrader, FractTradeHelper


class KalmanFilter:
    def __init__(self, x_hat0, v_hat0, v_sys, v_obs):
        self.x_hat = x_hat0     # a posteri estimate of x
        self.v_hat = v_hat0     # a posteri error estimate
        self.v_sys = v_sys      # process variance
        self.v_obs = v_obs      # estimate of measurement variance

    def update(self, x):
        x_hat_m = self.x_hat                    # a priori estimate of x
        v_hat_m = self.v_hat + self.v_sys       # a priori error estimate
        k = v_hat_m / (v_hat_m + self.v_obs)    # gain or blending factor
        self.x_hat = x_hat_m + k * (x - x_hat_m)
        self.v_hat = (1 - k) * v_hat_m
        return self.x_hat

    def update_offline(self, x_array):
        return np.array([self.update(x=x) for x in x_array])


class Kalman(FractTrader):
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
                    threshold = np.float32(
                        ws['std'] * self.model['sigma']['entry_trigger']
                    )
                    logging.debug('threshold: {}'.format(threshold))

                    k = KalmanFilter(x_hat0=ws['first'],
                                     v_hat0=ws['var'],
                                     v_sys=ws['var'],
                                     v_obs=ws['var'])
                    x_hat = k.update_offline(x_array=wi['midpoints'])[-1]
                    logging.debug('x_hat: {}'.format(x_hat))
                    x_delta = x_hat - ws['last']

                    if x_delta - threshold > 0:
                        helper.print_order_log(
                            response=self._place_order(sd=ws['std'],
                                                       prices=prices,
                                                       rate=rate,
                                                       side='buy',
                                                       units=units)
                        )
                    elif x_delta + threshold < 0:
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
