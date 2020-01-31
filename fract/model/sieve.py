#!/usr/bin/env python

import logging
import os

import numpy as np
import pandas as pd
import statsmodels.api as sm

from .feature import LogReturnFeature


class LRFeatureSieve(LogReturnFeature):
    def __init__(self, type, drop_zero=False, weight_decay=0):
        super().__init__(type=type, drop_zero=drop_zero)
        self.__logger = logging.getLogger(__name__)
        self.__weight_decay = weight_decay

    def extract_best_feature(self, history_dict, method='Ljung-Box'):
        feature_dict = {
            g: self.series(df_rate=d).dropna() for g, d in history_dict.items()
        }
        if len(feature_dict) <= 1:
            granularity = list(feature_dict.keys())[0]
        elif method == 'Ljung-Box':
            p_scores = pd.concat([
                pd.DataFrame({
                    'granularity': g,
                    'pvalue': sm.stats.diagnostic.acorr_ljungbox(x=s)[1]
                }).assign(
                    p_score=lambda d: d['pvalue'] / np.power(
                        1 - self.__weight_decay, np.arange(len(d))
                    )
                ) for g, s in feature_dict.items()
            ]).groupby('granularity').mean()['p_score']
            self.__logger.debug(f'p_scores:{os.linesep}{p_scores}')
            granularity = p_scores.idxmin()
        else:
            raise ValueError(f'invalid method name: {method}')
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
