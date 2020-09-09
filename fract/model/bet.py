#!/usr/bin/env python

import logging

import pandas as pd


class BettingSystem(object):
    def __init__(self, strategy='Martingale'):
        self.__logger = logging.getLogger(__name__)
        strategies = [
            'Martingale', 'Paroli', "d'Alembert", "Reverse d'Alembert",
            'Pyramid', "Oscar's grind"
        ]
        matched_st = [
            s for s in strategies
            if strategy.lower().replace("'", '') == s.lower().replace("'", '')
        ]
        if matched_st:
            self.strategy = matched_st[0]
            self.__logger.info(f'Betting strategy:\t{self.strategy}')
        else:
            raise ValueError('invalid strategy name')

    def calculate_size_by_pl(self, unit_size, inst_pl_txns, init_size=None):
        pl_list = [float(t['pl']) for t in inst_pl_txns if float(t['pl']) != 0]
        size_list = [
            abs(int(float(t['units']))) for t in inst_pl_txns
            if float(t['units']) != 0
        ]
        if not (pl_list and size_list):
            return init_size or unit_size
        else:
            last_size = size_list[-1]
            self.__logger.debug(f'last_size:\t{last_size}')
            if abs(pl_list[-1]):
                pl = pd.Series(pl_list)
                last_won = (
                    None if (
                        pl.iloc[-1] > 0 and any(pl.le(0)) and
                        pl[pl.index >= pl[pl.le(0)].index.max()].sum() < 0
                    ) else (pl.iloc[-1] > 0)
                )
                self.__logger.debug(f'last_won:\t{last_won}')
                return self._calculate_size(
                    unit_size=unit_size, init_size=init_size,
                    last_size=last_size, last_won=last_won,
                    all_time_high=(pl.cumsum().idxmax() == pl.index[-1])
                )
            else:
                return last_size

    def _calculate_size(self, unit_size, init_size=None, last_size=None,
                        last_won=None, all_time_high=False):
        if last_won is None:
            return last_size or init_size or unit_size
        elif self.strategy == 'Martingale':
            return (unit_size if last_won else last_size * 2)
        elif self.strategy == 'Paroli':
            return (last_size * 2 if last_won else unit_size)
        elif self.strategy == "d'Alembert":
            return (unit_size if last_won else last_size + unit_size)
        elif self.strategy == "Reverse d'Alembert":
            return (last_size + unit_size if last_won else unit_size)
        elif self.strategy == 'Pyramid':
            if not last_won:
                return (last_size + unit_size)
            elif last_size < unit_size:
                return last_size
            else:
                return (last_size - unit_size)
        elif self.strategy == "Oscar's grind":
            self.__logger.debug(f'all_time_high:\t{all_time_high}')
            if all_time_high:
                return init_size or unit_size
            elif last_won:
                return (last_size + unit_size)
            else:
                return last_size
        else:
            raise ValueError('invalid strategy name')
