#!/usr/bin/env python

from ..util.error import FractRuntimeError


class BettingSystem(object):
    def __init__(self, strategy, init_to_unit_ratio=1):
        strategies = ['Martingale', "d'Alembert", 'Pyramid', "Oscar's grind"]
        if strategy in strategies:
            self.strategy = strategy
        else:
            raise FractRuntimeError('invalid strategy name')

    def calculate_size(self, unit_size, init_size=None, last_size=None,
                       last_won=None, is_all_time_high=False):
        if last_size is None:
            return init_size or unit_size
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
            if is_all_time_high:
                return init_size or unit_size
            elif last_won:
                return (last_size + unit_size)
            else:
                return last_size
        else:
            raise FractRuntimeError('invalid strategy name')
