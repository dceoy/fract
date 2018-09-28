#!/usr/bin/env python

from datetime import datetime
import logging
import numpy as np
from .base import FractTrader, FractTradeHelper


class KalmanFilter:
    def __init__(self, x_hat0, v_err0, v_sys_err, v_obs_err):
        self.x_hat = np.array([x_hat0])     # a posteri estimate of x
        self.v_err = np.array([v_err0])     # a posteri error estimate
        self.v_sys_err = v_sys_err          # process variance
        self.v_obs_err = v_obs_err          # estimate of measurement variance

    def update(self, x):
        x_hat_m = self.x_hat[-1]                    # a priori estimate of x
        v_err_m = self.v_err[-1] + self.v_sys_err   # a priori error estimate
        k = v_err_m / (v_err_m + self.v_obs_err)    # gain or blending factor
        self.x_hat = np.append(self.x_hat, x_hat_m + k * (x - x_hat_m))
        self.v_err = np.append(self.v_err, (1 - k) * v_err_m)
        return self.x_hat[-1]

    def update_offline(self, x_array):
        [self.update(x=x) for x in x_array]
        return self.x_hat[-1]


class Kalman(FractTrader):
    def __init__(self, oanda, margin_ratio, model, quiet=False):
        super().__init__(oanda=oanda,
                         margin_ratio=margin_ratio,
                         model=model,
                         quiet=quiet)

    def _kalman_filter(self, window):
        return (
            lambda v0:
            KalmanFilter(
                x_hat0=window['midpoints'][0],
                v_err0=v0,
                v_sys_err=v0 * self.model['error']['sys_var'],
                v_obs_err=v0 * self.model['error']['obs_var']
            )
        )(
            v0=window['midpoints'][
                -int(self.model['error']['ref_window']):
            ].var(ddof=1)
        )

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
                    kf = self._kalman_filter(window=wi)
                    kf.update_offline(x_array=wi['midpoints'])
                    logging.debug('kf.x_hat: {}'.format(kf.x_hat))

                    x_delta = np.float32(kf.x_hat[-1] - kf.x_hat[-2])
                    threshold = np.float32(
                        ws['std'] * self.model['sigma']['entry_trigger']
                    )
                    logging.debug('x_delta: {0}, threshold: {1}'.format(
                        x_delta, threshold
                    ))

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
