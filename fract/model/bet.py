#!/usr/bin/env python

import logging
from ..util.error import FractRuntimeError


class BettingSystem(object):
    def __init__(self, strategy='Martingale'):
        self.logger = logging.getLogger(__name__)
        strategies = ['Martingale', "d'Alembert", 'Pyramid', "Oscar's grind"]
        if strategy in strategies:
            self.logger.info('Betting strategy: {}'.format(strategy))
            self.strategy = strategy
        else:
            raise FractRuntimeError('invalid strategy name')

    def calculate_size(self, unit_size, init_size=None, last_size=None,
                       last_won=None, all_time_high=False):
        self.logger.debug('last_won: {}'.format(last_won))
        self.logger.debug('last_size: {}'.format(last_size))
        if last_won is None or last_size is None:
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
