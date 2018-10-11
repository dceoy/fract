#!/usr/bin/env python

import logging
from pprint import pformat
import numpy as np
from scipy import stats
from .feature import LogReturnFeature
from .kvs import RedisTrader


class EwmaTrader(RedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def calculate_signal(self, instrument):
        mp = self.cf['model']['ewma']
        lrf = LogReturnFeature(df_rate=self.cache_dfs[instrument])
        fewm = lrf.series(type=self.cf['feature']).ewm(alpha=mp['alpha'])
        self.logger.debug('fewm: {}'.format(fewm))
        ewma = fewm.mean().iloc[-1]
        self.logger.info('EWMA of log return rate: {}'.format(ewma))
        n_ewm = len(fewm.obj.dropna())
        ewmci = (
            np.asarray(
                stats.t.interval(alpha=mp['ci_level'], df=(n_ewm - 1))
            ) * fewm.std().iloc[-1] / np.sqrt(n_ewm) + ewma
        )
        ci_level_pct = mp['ci_level'] * 100
        self.logger.info('EWMA {0}% CI: {1}'.format(ci_level_pct, ewmci))
        pos = self.pos_dict.get(instrument)
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
                self.feature_code, ci_level_pct, '{:1.5f}'.format(ewma),
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
