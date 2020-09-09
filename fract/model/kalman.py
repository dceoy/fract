#!/usr/bin/env python

import logging

import numpy as np
from scipy.stats import norm

from ..util.kalmanfilter import KalmanFilter, KalmanFilterOptimizer
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

    def detect_signal(self, history_dict, pos=None, contrary=False):
        best_f = self.__lrfs.extract_best_feature(history_dict=history_dict)
        kfo = KalmanFilterOptimizer(
            y=best_f['series'], x0=self.__x0, v0=self.__v0,
            pmv_ratio=self.__pmv_ratio
        )
        q, r = kfo.optimize()
        kf = KalmanFilter(x0=self.__x0, v0=self.__v0, q=q, r=r)
        kf_res = kf.fit(y=best_f['series']).iloc[-1].to_dict()
        self.__logger.debug(f'kf_res:\t{kf_res}')
        gauss_mu = kf_res['x']
        gauss_ci = np.asarray(
            norm.interval(
                alpha=self.__ci_level, loc=gauss_mu,
                scale=np.sqrt(kf_res['v'] + q)
            )
        )
        sig_side = 'short' if gauss_mu * [1, -1][int(contrary)] < 0 else 'long'
        if gauss_ci[1] < 0 or gauss_ci[0] > 0:
            sig_act = sig_side
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
        return {
            'sig_act': sig_act, 'granularity': best_f['granularity'],
            'sig_log_str': sig_log_str, 'sig_mu': gauss_mu,
            'sig_cil': gauss_ci[0], 'sig_ciu': gauss_ci[1]
        }
