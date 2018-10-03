#!/usr/bin/env python

from datetime import datetime
import json
import logging
import time
import pandas as pd
import redis
from .base import BaseTrader


class RedisTrader(BaseTrader):
    def __init__(self, config_dict, instruments, redis_host='127.0.0.1',
                 redis_port=6379, redis_db=0, redis_pool=None, interval_sec=1,
                 timeout_sec=3600, log_dir_path=None, quiet=False):
        self.logger = logging.getLogger(__name__)
        super().__init__(
            config_dict=config_dict, instruments=instruments,
            log_dir_path=log_dir_path, quiet=quiet
        )
        self.interval_sec = int(interval_sec)
        self.timeout_sec = int(timeout_sec) if timeout_sec else None
        self.redis_pool = redis_pool or redis.ConnectionPool(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.is_active = True
        self.latest_update_time = None

    def fetch_cached_rates(self, instrument):
        redis_c = redis.StrictRedis(connection_pool=self.redis_pool)
        cached_rates = [
            json.loads(s) for s in redis_c.lrange(instrument, 0, -1)
        ]
        if len(cached_rates) > 1:
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
