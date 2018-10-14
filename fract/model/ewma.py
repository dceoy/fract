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
        self.lrf = LogReturnFeature(
            type=config_dict['feature']['type'],
            spread_adjust=config_dict['feature']['spread_adjust']
        )

    def _ewm_stats(self, series):
        ewm = series.ewm(alpha=self.alpha)
        ewma = ewm.mean().iloc[-1]
        self.logger.debug('ewma: {}'.format(ewma))
        if self.ci_level:
            n_ewm = len(series)
            ewmci = np.asarray(
                stats.t.interval(alpha=self.ci_level, df=(n_ewm - 1))
            ) * ewm.std().iloc[-1] / np.sqrt(n_ewm) + ewma
            return {'ewma': ewma, 'ewmcil': ewmci[0], 'ewmciu': ewmci[1]}
        else:
            return {'ewma': ewma}

    def detect_signal(self, df_rate, df_candle, granularity, pos=None):
        ewm_dict = {
            **{
                'tick_{}'.format(k): v for k, v in self._ewm_stats(
                    series=self.lrf.series(df_rate=df_rate)
                ).items()
            },
            **{
                'close_{}'.format(k): v for k, v in self._ewm_stats(
                    series=self.lrf.series(
                        df_rate=df_candle.rename(
                            columns={'closeAsk': 'ask', 'closeBid': 'bid'}
                        )
                    )
                ).items()
            }
        }
        if self.ci_level:
            if pos and pos['side'] == 'buy' and ewm_dict['close_ewma'] < 0:
                sig_act = 'close'
            elif pos and pos['side'] == 'sell' and ewm_dict['close_ewma'] > 0:
                sig_act = 'close'
            elif ewm_dict['close_ewmcil'] > 0 and ewm_dict['tick_ewmcil'] > 0:
                sig_act = 'buy'
            elif ewm_dict['close_ewmcil'] > 0 and ewm_dict['tick_ewmciu'] < 0:
                sig_act = 'buy'
            elif ewm_dict['close_ewmciu'] < 0 and ewm_dict['tick_ewmciu'] < 0:
                sig_act = 'sell'
            elif ewm_dict['close_ewmciu'] < 0 and ewm_dict['tick_ewmcil'] > 0:
                sig_act = 'sell'
            else:
                sig_act = None
        else:
            if pos and pos['side'] == 'buy' and ewm_dict['close_ewma'] < 0:
                sig_act = 'close'
            elif pos and pos['side'] == 'sell' and ewm_dict['close_ewma'] > 0:
                sig_act = 'close'
            elif ewm_dict['close_ewma'] > 0 and ewm_dict['tick_ewma'] != 0:
                sig_act = 'buy'
            elif ewm_dict['close_ewma'] < 0 and ewm_dict['tick_ewma'] != 0:
                sig_act = 'sell'
            else:
                sig_act = None
        sig_log_str = '{0:^25}|{1:^23}|'.format(
            '{0:>3}[TICK]:{1:>11}'.format(
                self.lrf.code, '{:.3g}'.format(ewm_dict['tick_ewma'])
            ),
            '{0:>3}[{1}]:{2:>11}'.format(
                self.lrf.code, granularity,
                '{:.3g}'.format(ewm_dict['close_ewma'])
            )
        )
        return {'sig_act': sig_act, 'sig_log_str': sig_log_str, **ewm_dict}
