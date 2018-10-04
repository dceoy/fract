#!/usr/bin/env python

import logging
import numpy as np


class LogReturnFeature(object):
    def __init__(self, df_rate):
        self.logger = logging.getLogger(__name__)
        self.df_rate = df_rate

    def log_return(self, return_df=False):
        df_lr = self.df_rate.reset_index().assign(
            bid_by_ask=lambda d: d['bid'] / d['ask']
        ).assign(
            log_return=lambda d: np.log(d['mid']).diff(),
            spr_weight=lambda d: d['bid_by_ask'] / d['bid_by_ask'].mean(),
            delta_sec=lambda d: d['time'].diff().dt.total_seconds()
        )
        self.logger.info(
            'Adjusted log return (tail): {}'.format(
                df_lr['log_return'].tail().values
            )
        )
        return (df_lr if return_df else df_lr['log_return'])

    def log_return_velocity(self, return_df=False):
        df_lrv = self.log_return(return_df=True).assign(
            lrv=lambda d: d['log_return'] * d['spr_weight'] / d['delta_sec']
        )
        self.logger.info(
            'Adjusted log return verocity (tail): {}'.format(
                df_lrv['lrv'].tail().values
            )
        )
        return (df_lrv if return_df else df_lrv['lrv'])

    def log_return_acceleration(self, return_df=False):
        df_lra = self.log_return_velocity(return_df=True).assign(
            lra=lambda d: d['lrv'].diff() / d['delta_sec']
        )
        self.logger.info(
            'Adjusted log return acceleration (tail): {}'.format(
                df_lra['lra'].tail().values
            )
        )
        return (df_lra if return_df else df_lra['lra'])
