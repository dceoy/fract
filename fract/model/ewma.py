#!/usr/bin/env python

import logging
import numpy as np
from scipy import stats
from .feature import LogReturnFeature


class Ewma(object):
    def __init__(self, config_dict):
        self.logger = logging.getLogger(__name__)
        self.alpha = config_dict['model']['ewma']['alpha']
        self.ci_level = config_dict['model']['ewma'].get('ci_level')
        g = config_dict['feature']['granularity']
        self.gl_str = '{0:0>2}{1:1}'.format(int(g[1:] if len(g) else 1), g[0])
        self.lrf = LogReturnFeature(type=config_dict['feature']['type'])

    def detect_signal(self, df_rate, df_candle, pos=None):
        tick_dict = self._ewm_stats(series=self.lrf.series(df_rate=df_rate))
        close_dict = self._ewm_stats(
            series=self.lrf.series(
                df_rate=df_candle.rename(
                    columns={'closeAsk': 'ask', 'closeBid': 'bid'}
                )[['ask', 'bid']]
            )
        )
        if self.ci_level:
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
            sig_log_str = '{0:^44}|{1:^43}|'.format(
                '{0:>3}[TICK]:{1:>10}{2:>20}'.format(
                    self.lrf.code, '{:1.5f}'.format(tick_dict['ewma']),
                    np.array2string(
                        tick_dict['ewmci'],
                        formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
                    )
                ),
                '{0:>3}[{1:>3}]:{2:>10}{3:>20}'.format(
                    self.lrf.code, self.gl_str,
                    '{:1.5f}'.format(close_dict['ewma']),
                    np.array2string(
                        close_dict['ewmci'],
                        formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
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
            sig_log_str = '{0:^25}|{1:^24}|'.format(
                '{0:>3}[TICK]:{1:>11}'.format(
                    self.lrf.code, '{:.3g}'.format(tick_dict['ewma'])
                ),
                '{0:>3}[{1:>3}]:{2:>11}'.format(
                    self.lrf.code, self.gl_str,
                    '{:.3g}'.format(close_dict['ewma'])
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
        ewm = series.ewm(alpha=self.alpha)
        ewma = ewm.mean().iloc[-1]
        self.logger.debug('ewma: {}'.format(ewma))
        if self.ci_level:
            n_ewm = len(series)
            ewmci = np.asarray(
                stats.t.interval(alpha=self.ci_level, df=(n_ewm - 1))
            ) * ewm.std().iloc[-1] / np.sqrt(n_ewm) + ewma
            return {'ewma': ewma, 'ewmci': ewmci}
        else:
            return {'ewma': ewma}
