#!/usr/bin/env python

import logging
import numpy as np
from scipy import stats
from .sieve import LRFeatureSieve


class Ewma(object):
    def __init__(self, config_dict):
        self.__logger = logging.getLogger(__name__)
        self.__alpha = config_dict['model']['ewma']['alpha']
        self.__ci_level = config_dict['model']['ewma'].get('ci_level')
        self.__lrfs = LRFeatureSieve(type=config_dict['feature']['type'])

    def detect_signal(self, candle_dict, pos=None):
        best_f = self.__lrfs.extract_best_feature(candle_dict=candle_dict)
        close_dict = self._ewm_stats(series=best_f['series'])
        if pos and (
                (pos['side'] == 'buy' and close_dict['ewma'] < 0) or
                (pos['side'] == 'sell' and close_dict['ewma'] > 0)):
            sig_act = 'close'
        elif close_dict['ewmci'][0] > 0:
            sig_act = 'buy'
        elif close_dict['ewmci'][1] < 0:
            sig_act = 'sell'
        else:
            sig_act = None
        sig_log_str = '{:^40}|'.format(
            '{0:>3}[{1:>3}]:{2:>9}{3:>18}'.format(
                self.__lrf.code, best_f['ggranularity_str'],
                '{:.1g}'.format(close_dict['ewma']),
                np.array2string(
                    close_dict['ewmci'],
                    formatter={'float_kind': lambda f: '{:.1g}'.format(f)}
                )
            )
        )
        return {
            'sig_act': sig_act, 'sig_log_str': sig_log_str,
            'close_ewma': close_dict['ewma'],
            'close_ewmcil': close_dict['ewmci'][0],
            'close_ewmciu': close_dict['ewmci'][1]
        }

    def _ewm_stats(self, series):
        ewm = series.ewm(alpha=self.__alpha)
        ewma = ewm.mean().iloc[-1]
        self.__logger.debug('ewma: {}'.format(ewma))
        n_ewm = len(series)
        ewmci = np.asarray(
            stats.t.interval(alpha=self.__ci_level, df=(n_ewm - 1))
        ) * ewm.std().iloc[-1] / np.sqrt(n_ewm) + ewma
        return {'ewma': ewma, 'ewmci': ewmci}
