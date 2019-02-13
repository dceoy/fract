#!/usr/bin/env python

import logging
import numpy as np
from scipy import stats
from .sieve import granularity2str
from .feature import LogReturnFeature


class Ewma(object):
    def __init__(self, config_dict):
        self.__logger = logging.getLogger(__name__)
        self.__alpha = config_dict['model']['ewma']['alpha']
        self.__ci_level = config_dict['model']['ewma'].get('ci_level')
        self.__lrf = LogReturnFeature(type=config_dict['feature']['type'])

    def detect_signal(self, df_rate, df_candle, granularity, pos=None):
        gl_str = granularity2str(granularity=granularity)
        tick_dict = self._ewm_stats(series=self.__lrf.series(df_rate=df_rate))
        close_dict = self._ewm_stats(
            series=self.__lrf.series(
                df_rate=df_candle.rename(
                    columns={'closeAsk': 'ask', 'closeBid': 'bid'}
                )[['ask', 'bid']]
            )
        )
        if self.__ci_level:
            if pos and pos['side'] == 'buy' and close_dict['ewma'] < 0:
                sig_act = 'close'
            elif pos and pos['side'] == 'sell' and close_dict['ewma'] > 0:
                sig_act = 'close'
            elif close_dict['ewmci'][0] > 0 and tick_dict['ewmci'][0] > 0:
                sig_act = 'buy'
            elif close_dict['ewmci'][0] > 0 and tick_dict['ewmci'][1] < 0:
                sig_act = 'buy'
            elif close_dict['ewmci'][1] < 0 and tick_dict['ewmci'][1] < 0:
                sig_act = 'sell'
            elif close_dict['ewmci'][1] < 0 and tick_dict['ewmci'][0] > 0:
                sig_act = 'sell'
            else:
                sig_act = None
            sig_log_str = '{0:^41}|{1:^40}|'.format(
                '{0:>3}[TICK]:{1:>9}{2:>18}'.format(
                    self.__lrf.code, '{:.1g}'.format(tick_dict['ewma']),
                    np.array2string(
                        tick_dict['ewmci'],
                        formatter={'float_kind': lambda f: '{:.1g}'.format(f)}
                    )
                ),
                '{0:>3}[{1:>3}]:{2:>9}{3:>18}'.format(
                    self.__lrf.code, gl_str,
                    '{:.1g}'.format(close_dict['ewma']),
                    np.array2string(
                        close_dict['ewmci'],
                        formatter={'float_kind': lambda f: '{:.1g}'.format(f)}
                    )
                )
            )
        else:
            if pos and pos['side'] == 'buy' and close_dict['ewma'] < 0:
                sig_act = 'close'
            elif pos and pos['side'] == 'sell' and close_dict['ewma'] > 0:
                sig_act = 'close'
            elif close_dict['ewma'] > 0 and tick_dict['ewma'] != 0:
                sig_act = 'buy'
            elif close_dict['ewma'] < 0 and tick_dict['ewma'] != 0:
                sig_act = 'sell'
            else:
                sig_act = None
            sig_log_str = '{0:^24}|{1:^23}|'.format(
                '{0:>3}[TICK]:{1:>10}'.format(
                    self.__lrf.code, '{:.2g}'.format(tick_dict['ewma'])
                ),
                '{0:>3}[{1:>3}]:{2:>10}'.format(
                    self.__lrf.code, gl_str,
                    '{:.2g}'.format(close_dict['ewma'])
                )
            )
        return {
            'sig_act': sig_act, 'sig_log_str': sig_log_str,
            'tick_ewma': tick_dict['ewma'],
            'tick_ewmcil': tick_dict['ewmci'][0],
            'tick_ewmciu': tick_dict['ewmci'][1],
            'close_ewma': close_dict['ewma'],
            'close_ewmcil': close_dict['ewmci'][0],
            'close_ewmciu': close_dict['ewmci'][1]
        }

    def _ewm_stats(self, series):
        ewm = series.ewm(alpha=self.__alpha)
        ewma = ewm.mean().iloc[-1]
        self.__logger.debug('ewma: {}'.format(ewma))
        if self.__ci_level:
            n_ewm = len(series)
            ewmci = np.asarray(
                stats.t.interval(alpha=self.__ci_level, df=(n_ewm - 1))
            ) * ewm.std().iloc[-1] / np.sqrt(n_ewm) + ewma
            return {'ewma': ewma, 'ewmci': ewmci}
        else:
            return {'ewma': ewma}
