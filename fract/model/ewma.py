#!/usr/bin/env python

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import reduce
import json
import logging
from pprint import pformat
import signal
import time
import numpy as np
import oandapy
import pandas as pd
import redis
from ..trade.streamer import StreamDriver


class FractRedisTrader(oandapy.API):
    def __init__(self, config_dict, instruments, redis_host='127.0.0.1',
                 redis_port=6379, redis_db=0, interval=1, timeout=3600,
                 n_cpu=1, quiet=False, with_streamer=False):
        self.logger = logging.getLogger(__name__)
        self.cf = config_dict
        super().__init__(
            environment=config_dict['oanda']['environment'],
            access_token=config_dict['oanda']['access_token']
        )
        self.account_id = config_dict['oanda']['account_id'],
        self.oanda = oandapy.API(
            environment=config_dict['oanda']['environment'],
            access_token=config_dict['oanda']['access_token']
        )
        self.account_currency = self.get_account(
            account_id=self.account_id
        )['accountCurrency']
        self.instruments = (
            instruments if instruments else config_dict['instruments']
        )
        self.quiet = quiet
        self.tradable_instruments = [
            d['instrument'] for d in
            self.get_instruments(account_id=self.account_id)['instruments']
        ]
        self.interval = interval
        self.timeout = timeout
        self.n_cpu = n_cpu
        self.is_active = True
        self.df_rate = pd.DataFrame()
        self.executor = None
        if with_streamer:
            self.streamer = StreamDriver(
                config_dict=self.cf, target='rate', instruments=instruments,
                use_redis=True, redis_host=redis_host, redis_port=redis_port,
                redis_db=redis_db, quiet=True
            )
            self.redis_pool = self.redis_pool
        else:
            self.streamer = None
            self.redis_pool = redis.ConnectionPool(
                host=redis_host, port=redis_port, db=redis_db
            )
        self.redis = redis.StrictRedis(connection_pool=self.redis_pool)
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
        if self.executor:
            self.executor.shutdown(wait=False)
        if self.streamer:
            self.streamer.disconnect()
        self.redis.connection_pool.disconnect()

    def fetch_cached_rates(self, instrument):
        cached_rates = [
            json.loads(s) for s in self.redis.lrange(instrument, 0, -1)
        ]
        if cached_rates:
            self._redis_lpop(instrument, len(cached_rates))
            if [r for r in cached_rates if 'disconnect' in r]:
                self.logger.warning(cached_rates)
                self.shutdown()
                return None
            else:
                self.logger.debug(cached_rates)
                return pd.DataFrame(
                    [d['tick'] for d in cached_rates if 'tick' in d]
                ).assign(
                    time=lambda d: pd.to_datetime(d['time']),
                    mid=lambda d: (d['ask'] + d['bid']) / 2,
                    spread=lambda d: d['ask'] - d['bid']
                ).set_index('time', drop=True)
        else:
            return None

    def _redis_lpop(self, key, times=1):
        return [self.redis.lpop(key) for i in range(times)]


class Ewm:
    def __init__(self, alpha, mu=None, sigma=None):
        self.alpha = alpha
        self.mean = mu
        self.std = sigma
        self.var = sigma ** 2 if sigma else None

    def update(self, array):
        ewma = pd.DataFrame(
            np.insert(arr=array, obj=0, values=self.mean)
            if self.mean else array
        ).ewm(alpha=self.alpha, adjust=False).mean()[0].values
        self.mean = ewma[-1]
        self.var = reduce(
            lambda x0, x1: (1 - self.alpha) * (x0 + self.alpha * x1),
            (
                np.insert(arr=(array - ewma[:-1]) ** 2, obj=0, values=self.std)
                if self.var else (array[1:] - ewma[:-1]) ** 2
            )
        )
        self.std = np.sqrt(self.var)


class EwmLogDiffTrader(FractRedisTrader):
    def __init__(self, len_cache=10, **kwargs):
        super().__init__(**kwargs)
        self.op = self.cf['order']
        self.mp = self.cf['model']['ewma']
        self.len_cache = len_cache
        self.rate_dfs = {k: pd.DataFrame() for k in self.instruments}
        self.ewm_ld = {}

    def invoke(self):
        if self.streamer:
            self.executor = self.create_executor()
            fut = self.executor.submit(self._stream)
            fut.add_done_callback(self.shutdown)
        if not self.quiet:
            print('!!! OPEN A DEAL !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        try:
            self._open_deals()
        except Exception as e:
            self.shutdown()
            raise e
        else:
            self.shutdown()

    def _open_deals(self):
        while self.is_active:
            time.sleep(self.interval)
            for inst in self.instruments:
                df_new = self.fetch_cached_rates(instrument=inst)
                if df_new is not None:
                    self._trade(instrument=inst, df_new=df_new)

    def _trade(self, instrument, df_new):
        self.rate_dfs[instrument] = pd.concat(
            [self.rate_dfs[instrument], df_new]
        ).tail(n=self.len_cache)
        if len(self.rate_dfs[instrument]) < self.len_cache:
            return None
        else:
            if instrument in self.ewm_ld:
                self.ewm_ld[instrument].update(array=df_new[['mid']].values)
            else:
                ewm_ld = self.rate_dfs[instrument].pipe(
                    lambda d: np.log(d['mid']).diff()
                ).ewm(alpha=self.mp['alpha'], adjust=False)
                self.ewm_ld[instrument] = Ewm(
                    alpha=self.mp['alpha'], mu=ewm_ld.mean().values[-1],
                    sigma=ewm_ld.std().values[-1]
                )
            print(vars(self.ewm_ld[instrument]), flush=True)
