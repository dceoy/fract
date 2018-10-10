#!/usr/bin/env python

from abc import ABCMeta, abstractmethod
from datetime import datetime
import json
import logging
import os
import signal
import time
import numpy as np
import pandas as pd
import redis
from .base import BaseTrader


class RedisTrader(BaseTrader, metaclass=ABCMeta):
    def __init__(self, config_dict, instruments, redis_host='127.0.0.1',
                 redis_port=6379, redis_db=0, redis_pool=None, interval_sec=1,
                 timeout_sec=3600, log_dir_path=None, quiet=False):
        super().__init__(
            config_dict=config_dict, instruments=instruments,
            log_dir_path=log_dir_path, quiet=quiet
        )
        self.logger = logging.getLogger(__name__)
        self.interval_sec = int(interval_sec)
        self.timeout_sec = int(timeout_sec) if timeout_sec else None
        self.cache_min_len = self.cf['cache']['min_len']
        self.cache_max_len = self.cf['cache']['max_len']
        self.feature_code = (
            self.cf['feature'].replace(' ', '').upper()[:3]
            if self.cf['feature'].lower().startswith('lr ') else
            ''.join([c for c in self.cf['feature'].title() if c.isupper()])
        )
        self.redis_pool = redis_pool or redis.ConnectionPool(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.is_active = True
        self.latest_update_time = None
        self.cache_dfs = {i: pd.DataFrame() for i in self.instruments}

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self._check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            for i in self.instruments:
                self.refresh_oanda_dicts()
                df_r = self._fetch_cached_rates(instrument=i)
                if df_r.size:
                    self._update_caches(instrument=i, df_rate=df_r)
                    self.trade(instrument=i)
                    self.write_log_df(name='rate.{}'.format(i), df=df_r)
                else:
                    self.logger.debug('no updated rate')

    @abstractmethod
    def trade(self, instrument):
        pass

    def _check_health(self):
        if not self.latest_update_time:
            return self.is_active
        elif not self.is_active:
            self.redis_pool.disconnect()
            return self.is_active
        else:
            td = datetime.now() - self.latest_update_time
            if self.timeout_sec and td.total_seconds() > self.timeout_sec:
                self.logger.warning(
                    'Timeout: no data update ({} sec)'.format(self.timeout_sec)
                )
                self.is_active = False
                self.redis_pool.disconnect()
            else:
                time.sleep(self.interval_sec)
            return self.is_active

    def _fetch_cached_rates(self, instrument):
        redis_c = redis.StrictRedis(connection_pool=self.redis_pool)
        cached_rates = [
            json.loads(s) for s in redis_c.lrange(instrument, 0, -1)
        ]
        if len(cached_rates) > 0:
            self.latest_update_time = datetime.now()
            for i in cached_rates:
                redis_c.lpop(instrument)
            if [r for r in cached_rates if 'disconnect' in r]:
                self.logger.warning('cached_rates: {}'.format(cached_rates))
                self.is_active = False
                return pd.DataFrame()
            else:
                self.logger.debug('cached_rates: {}'.format(cached_rates))
                return pd.DataFrame(
                    [d['tick'] for d in cached_rates if 'tick' in d]
                ).assign(
                    time=lambda d: pd.to_datetime(d['time']),
                    spread=lambda d: d['ask'] - d['bid'],
                    mid=lambda d: (d['ask'] + d['bid']) / 2
                ).set_index('time', drop=True)
        else:
            return pd.DataFrame()

    def _update_caches(self, instrument, df_rate):
        self.logger.info('Rate:{0}{1}'.format(os.linesep, df_rate))
        df_c = self.cache_dfs[instrument]
        df_r = df_c.append(df_rate).tail(n=self.cache_max_len)
        self.logger.info('Cache length: {}'.format(len(df_r)))
        self.cache_dfs[instrument] = df_r

    def determine_order(self, instrument, side=None):
        pp = self.cf['position']
        pos = self.pos_dict.get(instrument)
        load_pct = int(
            min(1, len(self.cache_dfs[instrument]) / self.cache_min_len) * 100
        )
        margin_lack = (
            not pos and self.acc_dict['marginAvail'] <
            self.acc_dict['balance'] * pp['margin_nav_ratio']['preserve']
        )
        spread_ratio = self.cache_dfs[instrument].tail(n=1).pipe(
            lambda d: (d['spread'] / d['mid']).iloc[-1]
        )
        pos_pct = (
            pos['units'] * self.unit_costs[instrument] * 100 /
            self.acc_dict['balance']
            if pos else 0
        )
        if load_pct < 100:
            return {'act': None, 'state': 'LOADING {:>3}%'.format(load_pct)}
        elif self.inst_dict[instrument]['halted']:
            return {'act': None, 'state': 'TRADING HALTED'}
        elif self.acc_dict['balance'] == 0:
            return {'act': None, 'state': 'NO FUND'}
        elif margin_lack:
            return {'act': None, 'state': 'LACK OF FUNDS'}
        elif spread_ratio > pp['limit_price_ratio']['max_spread']:
            return {'act': None, 'state': 'OVER-SPREAD'}
        elif side == 'buy':
            if pos and pos['side'] == 'buy':
                return {'act': None, 'state': '{:.2g}% LONG'.format(pos_pct)}
            elif pos and pos['side'] == 'sell':
                return {'act': 'buy', 'state': 'SHORT -> LONG'}
            else:
                return {'act': 'buy', 'state': '-> LONG'}
        elif side == 'sell':
            if pos and pos['side'] == 'sell':
                return {'act': None, 'state': '{:.2g}% SHORT'.format(pos_pct)}
            elif pos and pos['side'] == 'buy':
                return {'act': 'sell', 'state': 'LONG -> SHORT'}
            else:
                return {'act': 'sell', 'state': '-> SHORT'}
        elif pos and pos['side'] == 'buy':
            return {'act': None, 'state': '{:.2g}% LONG'.format(pos_pct)}
        elif pos and pos['side'] == 'sell':
            return {'act': None, 'state': '{:.2g}% SHORT'.format(pos_pct)}
        else:
            return {'act': None, 'state': '-'}

    def latest_rate_str(self, instrument):
        return '{0:>7} >>{1:>21}'.format(
            instrument.replace('_', '/'),
            np.array2string(
                self.cache_dfs[instrument][['bid', 'ask']].iloc[-1].values,
                formatter={'float_kind': lambda f: '{:8g}'.format(f)}
            )
        )
