#!/usr/bin/env python

import logging
import os

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar


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
        self.__logger.debug(f'measurement variance:\t{r}')
        q = r * self.__pmv_ratio
        self.__logger.debug(f'process variance:\t{q}')
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
