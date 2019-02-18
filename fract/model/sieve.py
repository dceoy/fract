#!/usr/bin/env python

import logging
import os
import pandas as pd
import statsmodels.api as sm
from ..util.error import FractRuntimeError
from .feature import LogReturnFeature


class LRFeatureSieve(LogReturnFeature):
    def __init__(self, type):
        super().__init__(type)
        self.__logger = logging.getLogger(__name__)

    def extract_best_feature(self, history_dict, method='Ljung-Box'):
        feature_dict = {
            g: self.series(df_rate=d).dropna() for g, d in history_dict.items()
        }
        if len(feature_dict) <= 1:
            granularity = list(feature_dict.keys())[0]
        elif method == 'Ljung-Box':
            df_pval = pd.concat([
                pd.DataFrame({
                    'granularity': g,
                    'pvalue': sm.stats.diagnostic.acorr_ljungbox(x=s)[1]
                }) for g, s in feature_dict.items()
            ]).groupby('granularity').median()['pvalue']
            self.__logger.debug('df_pval:{0}{1}'.format(os.linesep, df_pval))
            granularity = df_pval.idxmin()
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
