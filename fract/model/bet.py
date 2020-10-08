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
        size_list = [
            float(t['units']) for t in inst_pl_txns if float(t['units']) != 0
        ]
        last_size = abs(int(size_list[-1] if size_list else 0))
        self.__logger.debug(f'last_size:\t{last_size}')
        pl = pd.Series([
            t['pl'] for t in inst_pl_txns
        ]).astype(float).pipe(lambda a: a[a != 0])
        if pl.size == 0:
            return last_size or init_size or unit_size
        else:
            won_last = (
                None if (pl.size > 1 and pl.iloc[-1] > 0 and pl[-2:].sum() < 0)
                else (pl.iloc[-1] > 0)
            )
            self.__logger.debug(f'won_last:\t{won_last}')
            return self._calculate_size(
                unit_size=unit_size, init_size=init_size,
                last_size=last_size, won_last=won_last,
                all_time_high=(pl.cumsum().idxmax() == pl.index[-1])
            )

    def _calculate_size(self, unit_size, init_size=None, last_size=None,
                        won_last=None, all_time_high=False):
        if won_last is None:
            return last_size or init_size or unit_size
        elif self.strategy == 'Martingale':
            return (unit_size if won_last else last_size * 2)
        elif self.strategy == 'Paroli':
            return (last_size * 2 if won_last else unit_size)
        elif self.strategy == "d'Alembert":
            return (unit_size if won_last else last_size + unit_size)
        elif self.strategy == "Reverse d'Alembert":
            return (last_size + unit_size if won_last else unit_size)
        elif self.strategy == 'Pyramid':
            if not won_last:
                return (last_size + unit_size)
            elif last_size < unit_size:
                return last_size
            else:
                return (last_size - unit_size)
        elif self.strategy == "Oscar's grind":
            self.__logger.debug(f'all_time_high:\t{all_time_high}')
            if all_time_high:
                return init_size or unit_size
            elif won_last:
                return (last_size + unit_size)
            else:
                return last_size
        else:
            raise ValueError('invalid strategy name')
