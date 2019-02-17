#!/usr/bin/env python

import pandas as pd
import statsmodels.api as sm
from ..util.error import FractRuntimeError
from .feature import LogReturnFeature


class LRFeatureSieve(LogReturnFeature):
    def __init__(self, type):
        super().__init__(type)

    def extract_best_feature(self, history_dict, method='Ljung-Box'):
        feature_dict = {
            g: self.__lrf.series(df_rate=d) for g, d in history_dict.items()
        }
        if len(feature_dict) <= 1:
            granularity = list(feature_dict.keys())[0]
        elif self.method == 'Ljung-Box':
            granularity = pd.DataFrame([
                {
                    'granularity': g,
                    'pval': sm.stats.diagnostic.acorr_ljungbo(x=f)[1].median()
                } for g, f in feature_dict.items()
            ]).set_index('granularity')['pval'].idxmin()
        else:
            raise FractRuntimeError('invalid method name: {}'.format(method))
        return {
            'series': feature_dict[granularity], 'granularity': granularity,
            'granularity_str': self._granularity2str(granularity=granularity)
        }

    @staticmethod
    def _granularity2str(granularity='S5'):
        return (
            'TCK' if granularity == 'TICK'
            else '{0:0>2}{1:1}'.format(
                int(granularity[1:] if len(granularity) > 1 else 1),
                granularity[0]
            )
        )
