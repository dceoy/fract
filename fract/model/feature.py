#!/usr/bin/env python

import logging

import numpy as np


class LogReturnFeature(object):
    def __init__(self, type, drop_zero=False):
        self.__logger = logging.getLogger(__name__)
        self.__drop_zero = drop_zero
        if type and type.lower() == 'lr velocity':
            self.code = 'LRV'
        elif type and type.lower() == 'lr acceleration':
            self.code = 'LRA'
        elif type and type.lower() in ['lr', 'log return']:
            self.code = 'LR'
        else:
            raise ValueError(f'invalid feature type:\t{type}')

    def series(self, df_rate):
        if self.code == 'LRV':
            return self.log_return_velocity(df_rate=df_rate)
        elif self.code == 'LRA':
            return self.log_return_acceleration(df_rate=df_rate)
        else:
            return self.log_return(df_rate=df_rate)

    def log_return(self, df_rate, return_df=False):
        df_lr = df_rate.reset_index().assign(
            log_diff=lambda d: np.log(d[['ask', 'bid']].mean(axis=1)).diff(),
            delta_sec=lambda d: d['time'].diff().dt.total_seconds()
        ).assign(
            log_return=lambda d: self._weighted_log_diff(df=d)
        ).pipe(
            lambda d: (
                d.iloc[d['log_return'].to_numpy().nonzero()[0]]
                if self.__drop_zero else d
            )
        )
        self.__logger.info(
            'Log return (tail):\t{}'.format(df_lr['log_return'].tail().values)
        )
        return (df_lr if return_df else df_lr['log_return'])

    def _weighted_log_diff(self, df):
        return df.assign(
            weight=lambda d: np.reciprocal(
                np.log(d['ask']) - np.log(d['bid'])
            ).pipe(
                lambda s: (s / s.mean()) * (d['volume'] / d['volume'].mean())
            )
        ).pipe(
            lambda d: d['log_diff'] * d['weight']
        )

    def log_return_velocity(self, df_rate, return_df=False):
        df_lrv = self.log_return(
            df_rate=df_rate, return_df=True
        ).assign(
            lrv=lambda d: d['log_return'] / d['delta_sec']
        )
        self.__logger.info(
            'Log return verocity (tail):\t{}'.format(
                df_lrv['lrv'].tail().values
            )
        )
        return (df_lrv if return_df else df_lrv['lrv'])

    def log_return_acceleration(self, df_rate, return_df=False):
        df_lra = self.log_return_velocity(
            df_rate=df_rate, return_df=True
        ).assign(
            lra=lambda d: d['lrv'].diff() / d['delta_sec']
        )
        self.__logger.info(
            'Log return acceleration (tail):\t{}'.format(
                df_lra['lra'].tail().values
            )
        )
        return (df_lra if return_df else df_lra['lra'])
