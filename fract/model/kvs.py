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
        self.__logger = logging.getLogger(__name__)
        self.__interval_sec = int(interval_sec)
        self.__timeout_sec = int(timeout_sec) if timeout_sec else None
        self.__n_cache = self.__cf['feature']['cache_length']
        self.__granularity = self.__cf['feature']['granularity']
        self.__ai = self.create_ai(model=model)
        self.__redis_pool = redis.ConnectionPool(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.__is_active = True
        self.__latest_update_time = None
        self.__cache_dfs = {i: pd.DataFrame() for i in self.__instruments}
        self.__logger.debug('vars(self): ' + pformat(vars(self)))

    def check_health(self):
        if not self.__latest_update_time:
            return self.__is_active
        elif not self.__is_active:
            self.__redis_pool.disconnect()
            return self.__is_active
        else:
            td = datetime.now() - self.__latest_update_time
            if self.__timeout_sec and td.total_seconds() > self.__timeout_sec:
                self.__logger.warning(
                    'Timeout: no data update ({} sec)'.format(
                        self.__timeout_sec
                    )
                )
                self.__is_active = False
                self.__redis_pool.disconnect()
            else:
                time.sleep(self.__interval_sec)
            return self.__is_active

    def fetch_rate_df(self, instrument):
        redis_c = redis.StrictRedis(connection_pool=self.__redis_pool)
        cached_rates = [
            json.loads(s) for s in redis_c.lrange(instrument, 0, -1)
        ]
        if len(cached_rates) > 0:
            self.__latest_update_time = datetime.now()
            for i in cached_rates:
                redis_c.lpop(instrument)
            if [r for r in cached_rates if 'disconnect' in r]:
                self.__logger.warning('cached_rates: {}'.format(cached_rates))
                self.__is_active = False
                return pd.DataFrame()
            else:
                self.__logger.debug('cached_rates: {}'.format(cached_rates))
                return pd.DataFrame(
                    [d['tick'] for d in cached_rates if 'tick' in d]
                ).assign(
                    time=lambda d: pd.to_datetime(d['time'])
                ).set_index('time', drop=True)
        else:
            return pd.DataFrame()

    def update_caches(self, df_rate):
        self.__logger.info('Rate:{0}{1}'.format(os.linesep, df_rate))
        i = df_rate['instrument'].iloc[-1]
        df_c = self.__cache_dfs[i].append(df_rate).tail(n=self.__n_cache)
        self.__logger.info('Cache length: {}'.format(len(df_c)))
        self.__cache_dfs[i] = df_c

    def determine_sig_state(self, df_rate):
        i = df_rate['instrument'].iloc[-1]
        pos = self.__pos_dict.get(i)
        pos_pct = int(
            (
                pos['units'] * self.__unit_costs[i] * 100 /
                self.__acc_dict['balance']
            ) if pos else 0
        )
        sig = self.__ai.detect_signal(
            df_rate=self.__cache_dfs[i],
            df_candle=self.fetch_candle_df(
                instrument=i, granularity=self.__granularity,
                count=self.__n_cache
            ),
            pos=pos
        )
        if self.__inst_dict[i]['halted']:
            act = None
            state = 'TRADING HALTED'
        elif sig['sig_act'] == 'close':
            act = 'close'
            state = 'CLOSING'
        elif self.__acc_dict['balance'] == 0:
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
