#!/usr/bin/env python

import logging
from pprint import pformat
from .feature import LogReturnFeature
from .kvs import RedisTrader


class EwmaTrader(RedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def calculate_signal(self, instrument):
        lrf = LogReturnFeature(df_rate=self.cache_dfs[instrument])
        feature_ewm = lrf.series(type=self.cf['feature']).ewm(
            alpha=self.cf['model']['ewma']['alpha']
        )
        self.logger.debug('feature_ewm: {}'.format(feature_ewm))
        ewma = feature_ewm.mean().iloc[-1]
        self.logger.info('EWMA feature: {}'.format(ewma))
        if ewma > 0:
            sig_act = 'buy'
        elif ewma < 0:
            sig_act = 'sell'
        elif self.pos_dict.get(instrument):
            sig_act = 'close'
        else:
            sig_act = None
        sig_log_str = '{:^19}|'.format(
            '{0:>3}:{1:>11}'.format(self.feature_code, '{:.3g}'.format(ewma))
        )
        return {'ewma': ewma, 'sig_act': sig_act, 'sig_log_str': sig_log_str}
