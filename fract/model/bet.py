#!/usr/bin/env python

import logging
import pandas as pd
from ..util.error import FractRuntimeError


class BettingSystem(object):
    def __init__(self, strategy='Martingale'):
        self.logger = logging.getLogger(__name__)
        strategies = ['Martingale', "d'Alembert", 'Pyramid', "Oscar's grind"]
        matched_st = [
            s for s in strategies
            if strategy.lower().replace("'", '') == s.lower().replace("'", '')
        ]
        if matched_st:
            self.strategy = matched_st[0]
            self.logger.info('Betting strategy: {}'.format(self.strategy))
        else:
            raise FractRuntimeError('invalid strategy name')

    def calculate_size_by_pl(self, unit_size, init_size=None, inst_txns=[]):
        pl_txns = [t for t in inst_txns if 'pl' in t]
        if not pl_txns:
            return init_size or unit_size
        elif pl_txns[-1]['pl']:
            pl = pd.Series([t['pl'] for t in pl_txns if t['pl']])
            last_won = (
                None if (
                    pl.iloc[-1] > 0 and any(pl.le(0)) and
                    pl[pl.index >= pl[pl.le(0)].index.max()].sum() < 0
                ) else (pl.iloc[-1] > 0)
            )
            return self.calculate_size(
                unit_size=unit_size, init_size=init_size,
                last_size=pl_txns[-1]['units'], last_won=last_won,
                all_time_high=(pl.cumsum().idxmax() == pl.index[-1])
            )
        else:
            return pl_txns[-1]['units']

    def calculate_size(self, unit_size, init_size=None, last_size=None,
                       last_won=None, all_time_high=False):
        self.logger.debug('last_won: {}'.format(last_won))
        self.logger.debug('last_size: {}'.format(last_size))
        if last_won is None:
            return last_size or init_size or unit_size
        elif self.strategy == 'Martingale':
            return (unit_size if last_won else last_size * 2)
        elif self.strategy == "d'Alembert":
            return (unit_size if last_won else last_size + unit_size)
        elif self.strategy == 'Pyramid':
            if not last_won:
                return (last_size + unit_size)
            elif last_size < unit_size:
                return last_size
            else:
                return (last_size - unit_size)
        elif self.strategy == "Oscar's grind":
            self.logger.debug('all_time_high: {}'.format(all_time_high))
            if all_time_high:
                return init_size or unit_size
            elif last_won:
                return (last_size + unit_size)
            else:
                return last_size
        else:
            raise FractRuntimeError('invalid strategy name')
