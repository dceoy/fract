#!/usr/bin/env python

import logging
import time
from datetime import datetime
from pprint import pformat

from .base import BaseTrader


class StandaloneTrader(BaseTrader):
    def __init__(self, model, config_dict, instruments, interval_sec=1,
                 timeout_sec=3600, log_dir_path=None, ignore_api_error=False,
                 quiet=False, dry_run=False):
        super().__init__(
            model=model, standalone=True, ignore_api_error=ignore_api_error,
            config_dict=config_dict, instruments=instruments,
            log_dir_path=log_dir_path, quiet=quiet, dry_run=dry_run
        )
        self.__logger = logging.getLogger(__name__)
        self.__interval_sec = float(interval_sec)
        self.__timeout_sec = float(timeout_sec) if timeout_sec else None
        self.__latest_update_time = None
        self.__logger.debug('vars(self):\t' + pformat(vars(self)))

    def check_health(self):
        if not self.__latest_update_time:
            return True
        else:
            td = datetime.now() - self.__latest_update_time
            if self.__timeout_sec and td.total_seconds() > self.__timeout_sec:
                self.__logger.warning(f'Timeout:\t{self.__timeout_sec} sec')
                return False
            else:
                time.sleep(self.__interval_sec)
                return True

    def make_decision(self, instrument):
        df_r = self.fetch_latest_price_df(instrument=instrument)
        st = self.determine_sig_state(df_rate=df_r)
        self.print_state_line(df_rate=df_r, add_str=st['log_str'])
        self.design_and_place_order(instrument=instrument, act=st['act'])
        self.write_turn_log(
            df_rate=df_r,
            **{k: v for k, v in st.items() if not k.endswith('log_str')}
        )
        self.__latest_update_time = datetime.now()
