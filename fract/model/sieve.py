#!/usr/bin/env python

import logging
import warnings

import pandas as pd
import statsmodels.api as sm

from .feature import LogReturnFeature


class LRFeatureSieve(LogReturnFeature):
    def __init__(self, type, drop_zero=False):
        super().__init__(type=type, drop_zero=drop_zero)
        self.__logger = logging.getLogger(__name__)

    def extract_best_feature(self, history_dict, method='Ljung-Box'):
        feature_dict = {
            g: self.series(df_rate=d).dropna() for g, d in history_dict.items()
        }
        if len(history_dict) == 1:
            granularity = list(history_dict.keys())[0]
        elif method == 'Ljung-Box':
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', FutureWarning)
                df_g = pd.DataFrame([
                    {
                        'granularity': g,
                        'pvalue': sm.stats.diagnostic.acorr_ljungbox(x=s)[1][0]
                    } for g, s in feature_dict.items()
                ])
            best_g = df_g.pipe(lambda d: d.iloc[d['pvalue'].idxmin()])
            granularity = best_g['granularity']
            self.__logger.debug('p-value:\t{}'.format(best_g['pvalue']))
        else:
            raise ValueError(f'invalid method name:\t{method}')
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
