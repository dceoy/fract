#!/usr/bin/env python

from concurrent.futures import as_completed, ProcessPoolExecutor, \
    ThreadPoolExecutor
import json
import logging
from pprint import pformat
import signal
import time
import oandapy
import pandas as pd
import redis


class FractRedisTrader(oandapy.API):
    def __init__(self, environment, access_token, account_id, instruments,
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 wait=1, timeout=3600, n_cpu=1, quiet=False, streamer=None):
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
        self.redis_pool = None if streamer else redis.ConnectionPool(
            host=redis_host, port=redis_port, db=redis_db
        )
        self.redis = redis.StrictRedis(
            connection_pool=(self.redis_pool or streamer.redis_pool)
        )
        self.interval = wait
        self.timeout = timeout
        self.n_cpu = n_cpu
        self.is_active = True
        self.df_rate = pd.DataFrame()
        self.streamer = streamer
        self.executor = None
        self.logger.debug(pformat(vars(self)))

    def create_executor(self, **kwargs):
        if self.streamer:
            if self.n_cpu >= 3:
                return ProcessPoolExecutor(max_workers=3)
            else:
                return ThreadPoolExecutor(max_workers=3)
        else:
            if self.n_cpu >= 2:
                return ProcessPoolExecutor(max_workers=2)
            else:
                return ThreadPoolExecutor(max_workers=2)

    def shutdown(self):
        self.is_active = False
        if self.streamer:
            self.streamer.disconnect()
        self.redis.connection_pool.disconnect()

    def fetch_rate_cache(self, instrument):
        rate_cache = [
            json.loads(s) for s in self.redis.lrange(instrument, 0, -1)
        ]
        if rate_cache:
            self.logger.debug('rate_cache: {}'.format(rate_cache))
            if self.executor:
                fut = self.executor.submit(
                    self._redis_lpop, instrument, len(rate_cache)
                )
            else:
                self._redis_lpop(instrument, len(rate_cache))
                fut = None
            disconnected = ['disconnect' for r in rate_cache]
            df = (
                None if disconnected else pd.DataFrame(
                    [d['tick'] for d in rate_cache if 'tick' in d]
                ).assign(
                    time=lambda d: pd.to_datetime(d['time']),
                    mid=lambda d: (d['ask'] + d['bid']) / 2
                )
            )
            if fut:
                res = fut.result()
                self.logger.debug(res)
            if disconnected:
                self.logger.warning(disconnected)
                self.shutdown()
            return df
        else:
            return None

    def _redis_lpop(self, key, times=1):
        return [self.redis.lpop(key) for i in range(times)]


class Ewma(FractRedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.n_load = 20
        self.df_cache = pd.DataFrame()

    def run(self):
        self.executor = self.create_executor()
        fs = []
        if self.streamer:
            fs.append(self.executor.submit(self.streamer.run))
            time.sleep(self.interval)
        if not self.quiet:
            print('!!! OPEN A DEAL !!!')
        fs.append(self.executor.submit(self._invoke_trader))
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        try:
            fs_results = [f.result() for f in as_completed(fs)]
        except Exception as e:
            self.executor.shutdown(wait=False)
            self.shutdown()
            raise e
        else:
            self.shutdown()
            self.logger.debug(fs_results)

    def _invoke_trader(self):
        while self.is_active:
            for inst in self.instrument_list:
                df_rate = self.fetch_rate_cache(instrument=inst)
                print(df_rate, flush=True)

    def _calculate_ewm_div(self, df):
        pass
