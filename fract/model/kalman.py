#!/usr/bin/env python

import logging
import os

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from scipy.stats import norm

from .sieve import LRFeatureSieve


class Kalman(object):
    def __init__(self, config_dict, x0=0, v0=1e-8):
        self.__logger = logging.getLogger(__name__)
        self.__x0 = x0
        self.__v0 = v0
        self.__pmv_ratio = config_dict['model']['kalman']['pmv_ratio']
        self.__ci_level = 1 - config_dict['model']['kalman']['alpha']
        self.__lrfs = LRFeatureSieve(
            type=config_dict['feature']['type'], drop_zero=True
        )
        self.__granularity = None

    def detect_signal(self, history_dict, pos=None):
        best_f = self.__lrfs.extract_best_feature(
            history_dict=history_dict,
            granularities=(
                [self.__granularity] if self.__granularity else None
            )
        )
        kfo = KalmanFilterOptimizer(
            y=best_f['series'], x0=self.__x0, v0=self.__v0,
            pmv_ratio=self.__pmv_ratio
        )
        q, r = kfo.optimize()
        kf = KalmanFilter(x0=self.__x0, v0=self.__v0, q=q, r=r)
        kf_res = kf.fit(y=best_f['series']).iloc[-1].to_dict()
        self.__logger.debug(f'kf_res: {kf_res}')
        gauss_mu = kf_res['x']
        gauss_ci = np.asarray(
            norm.interval(
                alpha=self.__ci_level, loc=gauss_mu,
                scale=np.sqrt(kf_res['v'] + q)
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
                f'{gauss_mu:.1g}',
                np.array2string(
                    gauss_ci, formatter={'float_kind': lambda f: f'{f:.1g}'}
                )
            )
        )
        if not (pos and self.__granularity):
            self.__granularity = (best_f['granularity'] if sig_act else None)
        return {
            'sig_act': sig_act, 'sig_log_str': sig_log_str,
            'sig_mu': gauss_mu, 'sig_cil': gauss_ci[0], 'sig_ciu': gauss_ci[1]
        }


class KalmanFilter(object):
    def __init__(self, x0=0, v0=1e-8, q=1e-8, r=1e-8, keep_history=False):
        self.x = np.array([x0])                 # estimate of x
        self.v = np.array([v0])                 # error estimate
        self.q = q                              # process variance
        self.r = r                              # measurement variance
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


class KalmanFilterOptimizer(object):
    def __init__(self, y, x0=0, v0=1e-8, pmv_ratio=1, method='Golden'):
        self.__logger = logging.getLogger(__name__)
        self.y = y
        self.x0 = x0
        self.v0 = v0
        self.__pmv_ratio = pmv_ratio    # process / measurement variance ratio
        self.__method = method          # Brent | Bounded | Golden

    def optimize(self):
        res = minimize_scalar(
            fun=self._loss, args=(self.y, self.x0, self.v0, self.__pmv_ratio),
            method=self.__method
        )
        self.__logger.debug(f'{os.linesep}{res}')
        r = np.exp(res.x)
        self.__logger.debug(f'measurement variance: {r}')
        q = r * self.__pmv_ratio
        self.__logger.debug(f'process variance: {q}')
        return q, r

    @staticmethod
    def _loss(a, y, x0, v0, pmv_ratio=1):
        r = np.exp(a)
        return KalmanFilter(
            x0=x0, v0=v0, q=(r * pmv_ratio), r=r
        ).fit(y=y).pipe(
            lambda d: np.sum(
                np.log(d['v'] + r) + np.square(d['y'] - d['x']) / (d['v'] + r)
            )
        )
