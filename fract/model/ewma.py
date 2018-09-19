#!/usr/bin/env python

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
import logging
from pprint import pformat
import signal
import time
import oandapy
import pandas as pd
import redis
from ..cli.util import FractError


class FractRedisTrader(oandapy.API):
    def __init__(self, environment, access_token, account_id, instruments,
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 wait=1, timeout=3600, n_cpu=1, quiet=False):
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
        self.timeout = timeout
        self.n_cpu = n_cpu
        self.is_active = True
        self.df_rate = pd.DataFrame()
        self.streamer = None
        self.executor = None
        self.logger.debug(pformat(vars(self)))

    def set_streamer(self, streamer):
        self.streamer = streamer

    def create_executor(self, **kwargs):
        return (
            ProcessPoolExecutor(**kwargs) if self.n_cpu > 1
            else ThreadPoolExecutor(**kwargs)
        )

    def shutdown(self):
        if self.executor:
            self.executor.shutdown(wait=False)
        self.disconnect()
        self.redis.connection_pool.disconnect()

    def fetch_rate_cache(self, instrument):
        rate_cache = [
            json.loads(s) for s in self.redis.lrange(instrument, 0, -1)
        ]
        if rate_cache:
            self.logger.debug('rate_cache: {}'.format(rate_cache))
            with self.create_executor(max_workers=1) as x:
                fut = x.submit(self._redis_lpop, instrument, len(rate_cache))
                disconnected = ['disconnect' for r in rate_cache]
                df = (
                    None if disconnected else pd.DataFrame(
                        [d['tick'] for d in rate_cache if 'tick' in d]
                    ).assign(
                        time=lambda d: pd.to_datetime(d['time']),
                        mid=lambda d: (d['ask'] + d['bid']) / 2
                    )
                )
                res = fut.result()
                self.logger.debug(res)
            if disconnected:
                self.logger.error(disconnected)
                raise FractError('A streaming process is disconnected.')
            else:
                return df
        else:
            return None

    def _redis_lpop(self, key, times=1):
        return [self.redis.lpop(key) for i in range(times)]


class Ewma(FractRedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self):
        if self.streamer:
            self.executor = self.create_executor(max_workers=1)
            self.executor.submit(self._invoke_streamer)
            time.sleep(self.interval)
        if not self.quiet:
            print('!!! OPEN A DEAL !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self.is_active:
            try:
                self._update_ewma()
                self._trade()
            except Exception as e:
                self.shutdown()
                raise e
            else:
                time.sleep(self.interval)
        if self.streamer:
            self.shutdown()

    def _invoke_streamer(self):
        self.streamer.run()

    def _update_ewma(self):
        for inst in self.instrument_list:
            df_rate = self.fetch_rate_cache(instrument=inst)
            print(df_rate, flush=True)

    def _trade():
        pass
