#!/usr/bin/env python

from datetime import datetime
import json
import logging
import os
from pprint import pformat
import time
import pandas as pd
import redis
from .base import BaseTrader


class RedisTrader(BaseTrader):
    def __init__(self, model, config_dict, instruments, redis_host='127.0.0.1',
                 redis_port=6379, redis_db=0, interval_sec=1, timeout_sec=3600,
                 log_dir_path=None, quiet=False, dry_run=False):
        super().__init__(
            config_dict=config_dict, instruments=instruments,
            log_dir_path=log_dir_path, quiet=quiet, dry_run=dry_run
        )
        self.logger = logging.getLogger(__name__)
        self.interval_sec = int(interval_sec)
        self.timeout_sec = int(timeout_sec) if timeout_sec else None
        self.cache_min_len = self.cf['feature']['cache']['min_len']
        self.cache_max_len = self.cf['feature']['cache']['max_len']
        self.granularity = self.cf['feature']['granularity']
        self.ai = self.create_ai(model=model)
        self.redis_pool = redis.ConnectionPool(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.is_active = True
        self.latest_update_time = None
        self.cache_dfs = {i: pd.DataFrame() for i in self.instruments}
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def check_health(self):
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

    def fetch_rate_df(self, instrument):
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
                    time=lambda d: pd.to_datetime(d['time'])
                ).set_index('time', drop=True)
        else:
            return pd.DataFrame()

    def update_caches(self, df_rate):
        self.logger.info('Rate:{0}{1}'.format(os.linesep, df_rate))
        i = df_rate['instrument'].iloc[-1]
        df_c = self.cache_dfs[i].append(df_rate).tail(n=self.cache_max_len)
        self.logger.info('Cache length: {}'.format(len(df_c)))
        self.cache_dfs[i] = df_c

    def determine_sig_state(self, df_rate):
        i = df_rate['instrument'].iloc[-1]
        pos = self.pos_dict.get(i)
        pos_pct = int(
            pos['units'] * self.unit_costs[i] * 100 / self.acc_dict['balance']
            if pos else 0
        )
        sig = self.ai.detect_signal(
            df_rate=self.cache_dfs[i],
            df_candle=self.fetch_candle_df(
                instrument=i, granularity=self.granularity,
                count=self.cache_max_len
            ),
            pos=pos
        )
        len_cache = len(self.cache_dfs[i])
        if len_cache < self.cache_min_len:
            act = None
            state = 'LOADING...{:>3}%'.format(
                int(len_cache / self.cache_min_len * 100)
            )
        elif self.inst_dict[i]['halted']:
            act = None
            state = 'TRADING HALTED'
        elif sig['sig_act'] == 'close':
            act = 'close'
            state = 'CLOSING'
        elif self.acc_dict['balance'] == 0:
            act = None
            state = 'NO FUND'
        elif self.is_margin_lack(instrument=i):
            act = None
            state = 'LACK OF FUNDS'
        elif self.is_over_spread(df_rate=df_rate):
            act = None
            state = 'OVER-SPREAD'
        elif sig['sig_act'] == 'buy':
            if pos and pos['side'] == 'buy':
                act = None
                state = '{:.1f}% LONG'.format(pos_pct)
            elif pos and pos['side'] == 'sell':
                act = 'buy'
                state = 'SHORT -> LONG'
            else:
                act = 'buy'
                state = '-> LONG'
        elif sig['sig_act'] == 'sell':
            if pos and pos['side'] == 'sell':
                act = None
                state = '{:.1f}% SHORT'.format(pos_pct)
            elif pos and pos['side'] == 'buy':
                act = 'sell'
                state = 'LONG -> SHORT'
            else:
                act = 'sell'
                state = '-> SHORT'
        elif pos and pos['side'] == 'buy':
            act = None
            state = '{:.1f}% LONG'.format(pos_pct)
        elif pos and pos['side'] == 'sell':
            act = None
            state = '{:.1f}% SHORT'.format(pos_pct)
        else:
            act = None
            state = '-'
        log_str = (
            sig['sig_log_str'] + '{0:^14}|{1:^18}|'.format(
                'TICK:{:>5}'.format(len(df_rate)), state
            )
        )
        return {'act': act, 'state': state, 'log_str': log_str, **sig}
