#!/usr/bin/env python

import logging

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import norm

from .sieve import LRFeatureSieve


class Kalman(object):
    def __init__(self, config_dict, x0=0, v0=1):
        self.__logger = logging.getLogger(__name__)
        self.__x0 = x0
        self.__v0 = v0
        self.__ci_level = 1 - config_dict['model']['kalman']['alpha']
        self.__lrfs = LRFeatureSieve(type=config_dict['feature']['type'])

    def detect_signal(self, history_dict, pos=None):
        best_f = self.__lrfs.extract_best_feature(history_dict=history_dict)
        kfo = KalmanFilterOptimizer(
            y=best_f['series'], x0=self.__x0, v0=self.__v0
        )
        var_f = best_f['series'].pipe(lambda a: a.iloc[a.nonzero()[0]].var())
        kf_param = kfo.optimize(q0=var_f, r0=var_f).x
        self.__logger.debug('kf_param: {}'.format(kf_param))
        kf = KalmanFilter(
            x0=self.__x0, v0=self.__v0, q=kf_param[0], r=kf_param[1]
        )
        kf_res = kf.fit(y=best_f['series']).iloc[-1]
        self.__logger.debug('kf_res: {}'.format(kf_res))
        gauss_mu = kf_res['x']
        gauss_ci = np.asarray(
            norm.interval(
                alpha=self.__ci_level, loc=gauss_mu, scale=np.sqrt(kf_res['v'])
            )
        )
        if gauss_ci[0] > 0:
            sig_act = 'long'
        elif gauss_ci[1] < 0:
            sig_act = 'short'
        elif (pos
              and ((pos['side'] == 'long' and gauss_mu < 0) or
                   (pos['side'] == 'short' and gauss_mu > 0))):
            sig_act = 'closing'
        else:
            sig_act = None
        sig_log_str = '{:^40}|'.format(
            '{0:>3}[{1:>3}]:{2:>9}{3:>18}'.format(
                self.__lrfs.code, best_f['granularity_str'],
                '{:.1g}'.format(gauss_mu),
                np.array2string(
                    gauss_ci,
                    formatter={'float_kind': lambda f: '{:.1g}'.format(f)}
                )
            )
        )
        return {
            'sig_act': sig_act, 'sig_log_str': sig_log_str,
            'sig_mu': gauss_mu, 'sig_cil': gauss_ci[0], 'sig_ciu': gauss_ci[1]
        }


class KalmanFilter(object):
    def __init__(self, x0=0, v0=1, q=1, r=1, keep_history=False):
        self.x = np.array([x0])             # estimate of x
        self.v = np.array([v0])             # error estimate
        self.q = q                          # process variance
        self.r = r                          # measurement variance
        self.y = np.array([np.nan])
        self.__keep_history = keep_history

    def fit(self, y, x0=None, v0=None, q=None, r=None):
        x0_ = x0 or self.x[-1]
        v0_ = v0 or self.v[-1]
        q_ = q or self.q
        r_ = r or self.r
        len_y = len(y)
        new_x = np.empty(len_y)
        new_v = np.empty(len_y)
        for i, y_n in enumerate(y):
            x_n_1 = (new_x[i - 1] if i else x0_)
            v_n_1 = (new_v[i - 1] if i else v0_) + q_
            k = v_n_1 / (v_n_1 + r_)
            new_x[i] = x_n_1 + k * (y_n - x_n_1)
            new_v[i] = (1 - k) * v_n_1
        if self.__keep_history:
            self.x = np.append(self.x, new_x)
            self.v = np.append(self.v, new_v)
            self.y = np.append(self.y, y)
        else:
            self.x = np.array([new_x[-1]])
            self.v = np.array([new_v[-1]])
        return pd.DataFrame(
            {'y': y, 'x': new_x, 'v': new_v},
            index=(y.index if hasattr(y, 'index') else range(len_y))
        )

    def loglik(self, y):
        return self.fit(y=y).pipe(
            lambda d: np.sum(
                np.log(
                    norm.pdf(
                        x=(d['y'] - d['x']), loc=0,
                        scale=np.sqrt(d['v'] + self.r)
                    )
                )
            )
        )


class KalmanFilterOptimizer(object):
    def __init__(self, y, x0=0, v0=1, method='TNC'):
        self.y = y
        self.x0 = x0
        self.v0 = v0
        self.__method = method

    def optimize(self, q0, r0):
        np.seterr(invalid='ignore')
        return minimize(
            fun=self._loss, x0=[q0, r0], args=(self.y, self.x0, self.v0),
            method=self.__method
        )

    @staticmethod
    def _loss(p, y, x0, v0):
        kf = KalmanFilter(x0=x0, v0=v0, q=p[0], r=p[1])
        return kf.fit(y=y).pipe(
            lambda d: np.sum(
                np.log(d['v'] + p[1])
                + np.square(d['y'] - d['x']) / (d['v'] + p[1])
            )
        )
