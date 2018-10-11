#!/usr/bin/env python

import logging
import numpy as np


class LogReturnFeature(object):
    def __init__(self, df_rate):
        self.logger = logging.getLogger(__name__)
        self.df_rate = df_rate

    def series(self, type=None):
        if type and type.lower() == 'lr velocity':
            return self.log_return_velocity()
        elif type and type.lower() == 'lr acceleration':
            return self.log_return_acceleration()
        else:
            return self.log_return()

    def log_return(self, return_df=False):
        df_lr = self.df_rate.reset_index().assign(
            log_diff=lambda d: np.log(d['mid']).diff(),
            delta_sec=lambda d: d['time'].diff().dt.total_seconds()
        ).assign(
            log_return=lambda d: np.sign(d['log_diff']) * (
                np.abs(d['log_diff']) - np.log(d['ask']) + np.log(d['bid'])
            ).clip(lower=0)
        )
        self.logger.info(
            'Log return (tail): {}'.format(df_lr['log_return'].tail().values)
        )
        return (df_lr if return_df else df_lr['log_return'])

    def log_return_velocity(self, return_df=False):
        df_lrv = self.log_return(return_df=True).assign(
            lrv=lambda d: d['log_return'] / d['delta_sec']
        )
        self.logger.info(
            'Log return verocity (tail): {}'.format(
                df_lrv['lrv'].tail().values
            )
        )
        return (df_lrv if return_df else df_lrv['lrv'])

    def log_return_acceleration(self, return_df=False):
        df_lra = self.log_return_velocity(return_df=True).assign(
            lra=lambda d: d['lrv'].diff() / d['delta_sec']
        )
        self.logger.info(
            'Log return acceleration (tail): {}'.format(
                df_lra['lra'].tail().values
            )
        )
        return (df_lra if return_df else df_lra['lra'])
