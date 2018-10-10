#!/usr/bin/env python

import logging
from pprint import pformat
import numpy as np
import pandas as pd
from scipy import stats
from .feature import LogReturnFeature
from .kvs import RedisTrader


class EwmaTrader(RedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def trade(self, instrument):
        es = self._calcurate_ewma_stats(instrument=instrument)
        st = {
            **es,
            **self.determine_order(instrument=instrument, side=es['ewm_side']),
            **self.cache_dfs[instrument].tail(n=1).reset_index().T.to_dict()[0]
        }
        self._print_log_line(stat=st)
        if st['act']:
            self.design_and_place_order(instrument=instrument, side=st['act'])
        self.write_log_df(
            name='stat.{}'.format(instrument),
            df=pd.DataFrame([st]).set_index('time', drop=True)
        )

    def _calcurate_ewma_stats(self, instrument):
        alpha = self.cf['model']['ewma']['alpha']
        ci_level = self.cf['model']['ewma']['ci_level']
        lrf = LogReturnFeature(df_rate=self.cache_dfs[instrument])
        fewm = lrf.series(type=self.cf['feature']).ewm(alpha=alpha)
        self.logger.debug('fewm: {}'.format(fewm))
        ewma = fewm.mean().iloc[-1]
        self.logger.info('EWMA of log return rate: {}'.format(ewma))
        n_ewm = len(fewm.obj.dropna())
        ewmci = (
            np.asarray(stats.t.interval(alpha=ci_level, df=(n_ewm - 1))) *
            fewm.std().iloc[-1] / np.sqrt(n_ewm) + ewma
        )
        self.logger.info('EWMA {0}% CI: {1}'.format(ci_level * 100, ewmci))
        if ewmci[0] > 0:
            side = 'buy'
        elif ewmci[1] < 0:
            side = 'sell'
        else:
            side = None
        return {
            'ewma': ewma, 'ewmci_lower': ewmci[0], 'ewmci_upper': ewmci[1],
            'ewm_side': side
        }

    def _print_log_line(self, stat):
        self.print_log(
            '|{0:^35}|{1:^48}|{2:^18}|'.format(
                self.latest_rate_str(instrument=stat['instrument']),
                '{0:>3}[CI{1:.2g}] >>{2:>11}{3:>21}'.format(
                    self.feature_code,
                    self.cf['model']['ewma']['ci_level'] * 100,
                    '{:1.5f}'.format(stat['ewma']),
                    np.array2string(
                        np.array([stat['ewmci_lower'], stat['ewmci_upper']]),
                        formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
                    )
                ),
                stat['state']
            )
        )
