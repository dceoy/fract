#!/usr/bin/env python

import logging
import time
from datetime import datetime
from pprint import pformat

import pandas as pd
import redis
import ujson

from .base import BaseTrader


class RedisTrader(BaseTrader):
    def __init__(self, model, config_dict, instruments, redis_host='127.0.0.1',
                 redis_port=6379, redis_db=0, interval_sec=1, timeout_sec=3600,
                 log_dir_path=None, ignore_api_error=False, quiet=False,
                 dry_run=False):
        super().__init__(
            model=model, standalone=False, ignore_api_error=ignore_api_error,
            config_dict=config_dict, instruments=instruments,
            log_dir_path=log_dir_path, quiet=quiet, dry_run=dry_run
        )
        self.__logger = logging.getLogger(__name__)
        self.__interval_sec = float(interval_sec)
        self.__timeout_sec = float(timeout_sec) if timeout_sec else None
        self.__redis_pool = redis.ConnectionPool(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.__is_active = True
        self.__latest_update_time = None
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
                    'Timeout: {} sec'.format(self.__timeout_sec)
                )
                self.__is_active = False
                self.__redis_pool.disconnect()
            else:
                time.sleep(self.__interval_sec)
            return self.__is_active

    def make_decision(self, instrument):
        df_r = self._fetch_rate_df(instrument=instrument)
        if df_r.size:
            self.update_caches(df_rate=df_r)
            st = self.determine_sig_state(df_rate=df_r)
            self.print_state_line(df_rate=df_r, add_str=st['log_str'])
            self.design_and_place_order(instrument=instrument, act=st['act'])
            self.write_turn_log(
                df_rate=df_r,
                **{k: v for k, v in st.items() if not k.endswith('log_str')}
            )
            self.__latest_update_time = datetime.now()
        else:
            self.__logger.debug('no updated rate')

    def _fetch_rate_df(self, instrument):
        redis_c = redis.StrictRedis(connection_pool=self.__redis_pool)
        cached_rates = [
            ujson.loads(s) for s in redis_c.lrange(instrument, 0, -1)
        ]
        if len(cached_rates) > 0:
            for _ in cached_rates:
                redis_c.lpop(instrument)
            if [r for r in cached_rates if not r['tradeable']]:
                self.__logger.warning('cached_rates: {}'.format(cached_rates))
                self.__is_active = False
                return pd.DataFrame()
            else:
                self.__logger.debug('cached_rates: {}'.format(cached_rates))
                return pd.DataFrame([
                    {
                        'time': r['time'], 'bid': r['closeoutBid'],
                        'ask': r['closeoutAsk']
                    } for r in cached_rates
                ]).assign(
                    time=lambda d: pd.to_datetime(d['time']),
                    instrument=instrument
                ).set_index('time')
        else:
            return pd.DataFrame()
