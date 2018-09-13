#!/usr/bin/env python

import json
import logging
import os
from pprint import pformat
import redis
import time
import oandapy


class FractRedisTrader(oandapy.API):
    def __init__(self, environment, access_token, account_id, instruments,
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 wait=1, timeout=3600, quiet=False):
        super().__init__(environment=environment, access_token=access_token)
        self.logger = logging.getLogger(__name__)
        self.account_id = account_id
        self.account_currency = self.get_account(
            account_id=self.account_id
        )['accountCurrency']
        self.quiet = quiet
        self.instrument_list = [
            d['instrument'] for d in
            self.get_instruments(account_id=self.account_id)['instruments']
        ]
        self.redis = redis.StrictRedis(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.interval = wait
        self.is_active = True
        self.logger.debug(
            '{0}:{1}{2}'.format(
                self.__class__.__name__, os.linesep,
                pformat({
                    'self.account_id': self.account_id,
                    'self.account_currency': self.account_currency,
                    'self.margin_ratio': self.margin_ratio,
                    'self.model': self.model, 'self.quiet': self.quiet,
                    'self.instrument_list': self.instrument_list
                })
            )
        )


class Ewma(FractRedisTrader):
    def __init__(self, environment, access_token, account_id, instruments,
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 wait=1, timeout=3600, quiet=False):
        super().__init__(
            environment=environment, access_token=access_token,
            account_id=account_id, instruments=instruments,
            redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
            wait=wait, timeout=timeout, quiet=quiet
        )

    def _update_ewma(self):
        for inst in self.instrument_list:
            rate = [
                d['tick'] for d in [
                    json.loads(s) for s in self.redis.lrange(inst, 0, -1)
                ] if 'tick' in d
            ]
            print(rate)

    def _trade():
        pass

    def invoke(self):
        while self.is_active:
            self._update_ewma()
            self._trade()
            time.sleep(self.interval)
