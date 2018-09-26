#!/usr/bin/env python

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
import logging
from pprint import pformat
import signal
import time
import numpy as np
import oandapy
import pandas as pd
import redis
from scipy import stats
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


class EwmLogDiffTrader(FractRedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.op = self.cf['order']
        self.mp = self.cf['model']['ewm']
        self.ewmld = EwmLogDiffCache(
            alpha=self.mp['alpha'], ci_level=self.mp['ci_level'],
            max_spread_prop=self.op['max_spread'],
            size_range=self.mp['window'], ewm_adjust=False, quiet=self.quiet
        )

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
            [self._trade(instrument=i) for i in self.instruments]

    def _trade(self, instrument):
        df_new = self.fetch_cached_rates(instrument=instrument)
        if df_new is not None:
            self.ewmld.update(key=instrument, df_new=df_new)
            if self.ewmld.caches[instrument]['side']:
                pass
            else:
                pass


class EwmLogDiffCache:
    def __init__(self, alpha=0.01, size_range=(10, 1000), ci_level=0.99,
                 max_spread_prop=0.001, ewm_adjust=False, quiet=False):
        self.alpha = alpha
        self.size_range = size_range
        self.ci_level = ci_level
        self.max_spread_prop = max_spread_prop
        self.ewm_adjust = ewm_adjust
        self.quiet = quiet
        self.caches = {}

    def update(self, key, df_new):
        last_rate = df_new.tail(n=1).reset_index().T.to_dict()[0]
        series_mid = (
            self.caches[key]['series_mid'].append(df_new['mid'])
            if key in self.caches else df_new['mid']
        ).tail(n=self.size_range[1])
        if series_mid.size < self.size_range[0]:
            mu = sigma = np.nan
            ci = np.array([np.nan] * 2)
            side = None
            state = 'LOADING'
        else:
            ewm = np.log(series_mid).diff().ewm(
                alpha=self.alpha, adjust=self.ewm_adjust, ignore_na=True
            )
            mu = ewm.mean().values[-1]
            sigma = ewm.std().values[-1]
            ci = np.array(
                stats.norm.interval(alpha=self.ci_level, loc=mu, scale=sigma)
            )
            if last_rate['spread'] > last_rate['mid'] * self.max_spread_prop:
                side = None
                state = 'OVERSPREAD'
            else:
                if ci[0] > 0:
                    side = 'BUY'
                elif ci[1] < 0:
                    side = 'SELL'
                else:
                    side = None
                state = side or ''
        self.caches[key] = {
            'mean': mu, 'std': sigma, 'ci': ci, 'side': side,
            'series_mid': series_mid, **last_rate
        }
        msg = '| {0:7} | RATE: {1:>20} | LD: {2:>20} | {3:^10} |'.format(
            key,
            np.array2string(
                df_new[['bid', 'ask']].values[-1],
                formatter={'float_kind': lambda f: '{:8g}'.format(f)}
            ),
            np.array2string(
                ci, formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
            ),
            state
        )
        if self.quiet:
            self.logger.info(msg)
        else:
            print(msg, flush=True)
