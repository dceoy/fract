#!/usr/bin/env python

import logging
from pprint import pformat
import time
from .base import BaseTrader


class StandaloneTrader(BaseTrader):
    def __init__(self, model, config_dict, instruments, interval_sec=1,
                 log_dir_path=None, quiet=False, dry_run=False):
        super().__init__(
            model=model, config_dict=config_dict, instruments=instruments,
            log_dir_path=log_dir_path, quiet=quiet, dry_run=dry_run
        )
        self.__logger = logging.getLogger(__name__)
        self.__interval_sec = int(interval_sec)
        self.__n_cache = self.cf['feature']['cache_length']
        self.__granularities = [
            a for a in self.cf['feature']['granularities'] if a != 'TICK'
        ]
        self.__logger.debug('vars(self): ' + pformat(vars(self)))

    def check_health(self):
        time.sleep(self.__interval_sec)
        return True

    def make_decision(self, instrument):
        df_r = self.fetch_latest_rate_df(instrument=instrument)
        st = self.determine_sig_state(df_rate=df_r)
        self.print_state_line(df_rate=df_r, add_str=st['log_str'])
        self.design_and_place_order(instrument=instrument, act=st['act'])
        self.turn_log(
            df_rate=df_r,
            **{k: v for k, v in st.items() if not k.endswith('log_str')}
        )

    def fetch_history_dict(self, instrument):
        return {
            g: self.fetch_candle_df(
                instrument=instrument, granularity=g, count=self.__n_cache
            ).rename(
                columns={'closeAsk': 'ask', 'closeBid': 'bid'}
            )[['ask', 'bid']] for g in self.__granularities
        }
