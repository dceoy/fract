#!/usr/bin/env python

from datetime import datetime
import os
from pprint import pformat
import signal
import numpy as np
import pandas as pd
from .kvs import RedisTrader


class EwmaLogDiffTrader(RedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mp = self.cf['model']['ewma']
        self.cache_dfs = {i: pd.DataFrame() for i in self.instruments}
        self.ewma_stats = {i: dict() for i in self.instruments}
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self.check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            self._open_deals()

    def _open_deals(self):
        for i in self.instruments:
            self.refresh_oanda_dicts()
            df_r = self.fetch_cached_rates(instrument=i)
            if df_r.size:
                self.logger.info('Rate:{0}{1}'.format(os.linesep, df_r))
                self._update_caches(instrument=i, df_rate=df_r)
                st = self._determine_order_side(instrument=i)
                if st['act']:
                    self.design_and_place_order(instrument=i, side=st['act'])
                else:
                    self.logger.info('Current state: {}'.format(st['state']))
                self.logger.debug('st: {}'.format(st))
                df_s = pd.DataFrame([st]).set_index('time', drop=True)
            else:
                df_s = pd.DataFrame()
            log_paths = {
                os.path.join(self.log_dir_path, '{0}.{1}.tsv'.format(k, i)): v
                for k, v in {'rate': df_r, 'stat': df_s}.items()
                if self.log_dir_path and v.size
            }
            for p, d in log_paths.items():
                self.write_df_log(df=d, path=p)
                self.logger.info('Updated TSV log: {}'.format(p))

    def _update_caches(self, instrument, df_rate):
        self.latest_update_time = datetime.now()
        df_r = self.cache_dfs[instrument].append(
            df_rate.assign(
                spread=lambda d: d['ask'] - d['bid'],
                mid=lambda d: (d['ask'] + d['bid']) / 2
            )
        ).tail(n=int(self.mp['window_range'][1]))
        self.logger.info('Window size: {}'.format(len(df_r)))
        self.cache_dfs[instrument] = df_r
        log_return_rate = df_r.reset_index().assign(
            bid_by_ask=lambda d: d['bid'] / d['ask']
        ).assign(
            log_diff=lambda d: np.log(d['mid']).diff(),
            spr_weight=lambda d: d['bid_by_ask'] / d['bid_by_ask'].sum(),
            delta_sec=lambda d: d['time'].diff().dt.total_seconds()
        ).dropna().pipe(
            lambda d: d['log_diff'] * d['spr_weight'] / d['delta_sec']
        )
        self.logger.debug(
            'Adjusted log return per second (tail): {}'.format(
                log_return_rate.tail().values
            )
        )
        ewm = log_return_rate.ewm(alpha=self.mp['alpha'])
        self.logger.debug('ewm: {}'.format(ewm))
        ewma = ewm.mean().values[-1]
        self.logger.info('EWMA of log return rate: {}'.format(ewma))
        ewmstd = ewm.std().values[-1]
        ewmsi = ewma + np.array([-1, 1]) * ewmstd * self.mp['sigma_multiplier']
        self.logger.info(
            'EWMA {0} sigma interval: {1}'.format(
                self.mp['sigma_multiplier'], ewmsi
            )
        )
        self.ewma_stats[instrument] = {
            'ewma': ewma, 'ewmsi_lower': ewmsi[0], 'ewmsi_upper': ewmsi[1],
            **df_r.tail(n=1).reset_index().T.to_dict()[0]
        }

    def _determine_order_side(self, instrument):
        ec = self.ewma_stats[instrument]
        pp = self.cf['position']
        pos = self.pos_dict.get(instrument)
        margin_lack = (
            not pos and self.acc_dict['marginAvail'] <
            self.acc_dict['balance'] * pp['margin_nav_ratio']['preserve']
        )
        if len(self.cache_dfs[instrument]) < self.mp['window_range'][0]:
            st = {'act': None, 'state': 'LOADING'}
        elif self.inst_dict[instrument]['halted']:
            st = {'act': None, 'state': 'TRADING HALTED'}
        elif self.acc_dict['balance'] == 0:
            st = {'act': None, 'state': 'NO FUND'}
        elif margin_lack:
            st = {'act': None, 'state': 'LACK OF FUNDS'}
        elif ec['spread'] > ec['mid'] * pp['limit_price_ratio']['max_spread']:
            st = {'act': None, 'state': 'OVER-SPREAD'}
        elif ec['ewmsi_lower'] > 0:
            if pos and pos['side'] == 'buy':
                st = {'act': None, 'state': 'LONG'}
            elif pos and pos['side'] == 'sell':
                st = {'act': 'buy', 'state': 'SHORT >>> LONG'}
            else:
                st = {'act': 'buy', 'state': '>>> LONG'}
        elif ec['ewmsi_upper'] < 0:
            if pos and pos['side'] == 'sell':
                st = {'act': None, 'state': 'SHORT'}
            elif pos and pos['side'] == 'buy':
                st = {'act': 'sell', 'state': 'LONG >>> SHORT'}
            else:
                st = {'act': 'sell', 'state': '>>> SHORT'}
        elif pos and pos['side'] == 'buy':
            st = {'act': None, 'state': 'LONG'}
        elif pos and pos['side'] == 'sell':
            st = {'act': None, 'state': 'SHORT'}
        else:
            st = {'act': None, 'state': '-'}
        self.print_log(
            '|{0:^11}| PRICE:{1:>21} | LRR W/ {2}S:{3:>29} |{4:^16}|'.format(
                instrument,
                np.array2string(
                    np.array([ec['bid'], ec['ask']]),
                    formatter={'float_kind': lambda f: '{:8g}'.format(f)}
                ),
                int(self.mp['sigma_multiplier']),
                '{0:1.5f} {1}'.format(
                    ec['ewma'],
                    np.array2string(
                        np.array([ec['ewmsi_lower'], ec['ewmsi_upper']]),
                        formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
                    )
                ),
                st['state']
            )
        )
        return {**st, **ec}
