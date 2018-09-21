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
        if self.streamer:
            self.streamer.disconnect()
        self.redis.connection_pool.disconnect()

    def fetch_cached_rates(self, instrument):
        cached_rates = [
            json.loads(s) for s in self.redis.lrange(instrument, 0, -1)
        ]
        if cached_rates:
            self.logger.debug('cached_rates: {}'.format(cached_rates))
            if self.executor:
                fut = self.executor.submit(
                    self._redis_lpop, instrument, len(cached_rates)
                )
            else:
                self._redis_lpop(instrument, len(cached_rates))
                fut = None
            disconnected = ['disconnect' for r in cached_rates]
            df = (
                None if disconnected else pd.DataFrame(
                    [d['tick'] for d in cached_rates if 'tick' in d]
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
        self.op = self.cf['order']
        self.mp = self.cf['model']['ewma']
        self.n_load = 20
        self.df_cache = pd.DataFrame()
        self.ewma = {}

    def invoke(self):
        self.executor = self.create_executor()
        fs = []
        if self.streamer:
            fs.append(self.executor.submit(self.streamer.invoke))
        if not self.quiet:
            print('!!! OPEN A DEAL !!!')
        fs.append(self.executor.submit(self._open_deals))
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        try:
            fs_res = [f.result() for f in as_completed(fs)]
        except Exception as e:
            self.executor.shutdown(wait=False)
            self.shutdown()
            raise e
        else:
            self.shutdown()
            self.logger.debug(fs_res)

    def _open_deals(self):
        while self.is_active:
            time.sleep(self.interval)
            for inst in self.instruments:
                df_rate = self.fetch_cached_rates(instrument=inst)
                print(df_rate, flush=True)

    def _calculate_ewma(self, instrument, df):
        if instrument in self.ewma:
            pass
        else:
            pass
