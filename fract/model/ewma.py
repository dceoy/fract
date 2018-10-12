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

    def detect_signal(self, df_rate, pos=None):
        fewm = self.lrf.series(df_rate=df_rate).ewm(alpha=self.alpha)
        self.logger.debug('fewm: {}'.format(fewm))
        ewma = fewm.mean().iloc[-1]
        self.logger.info('EWMA feature: {}'.format(ewma))
        if self.ci_level:
            n_ewm = len(fewm.obj.dropna())
            ewmci = (
                np.asarray(
                    stats.t.interval(alpha=self.ci_level, df=(n_ewm - 1))
                ) * fewm.std().iloc[-1] / np.sqrt(n_ewm) + ewma
            )
            if ewmci[0] > 0:
                sig_act = 'buy'
            elif ewmci[1] < 0:
                sig_act = 'sell'
            elif pos and ewma * {'buy': 1, 'sell': -1}[pos['side']] < 0:
                sig_act = 'close'
            else:
                sig_act = None
            sig_log_str = '{:^46}|'.format(
                '{0:>3}[CI{1:.2g}]:{2:>11}{3:>21}'.format(
                    self.lrf.code, self.ci_level * 100, '{:1.5f}'.format(ewma),
                    np.array2string(
                        ewmci,
                        formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
                    )
                )
            )
            return {
                'ewma': ewma, 'ewmci_lower': ewmci[0], 'ewmci_upper': ewmci[1],
                'sig_act': sig_act, 'sig_log_str': sig_log_str
            }
        else:
            if ewma > 0:
                sig_act = 'buy'
            elif ewma < 0:
                sig_act = 'sell'
            elif pos:
                sig_act = 'close'
            else:
                sig_act = None
            sig_log_str = '{:^19}|'.format(
                '{0:>3}:{1:>11}'.format(self.lrf.code, '{:.3g}'.format(ewma))
            )
            return {
                'ewma': ewma, 'sig_act': sig_act, 'sig_log_str': sig_log_str
            }
