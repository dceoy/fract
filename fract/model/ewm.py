#!/usr/bin/env python

from datetime import datetime
import os
import signal
import numpy as np
import pandas as pd
from scipy import stats
from .kvs import RedisTrader


class EwmLogDiffTrader(RedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ewmld = EwmLogDiff(
            alpha=self.cf['model']['ewm']['alpha'],
            ci_level=self.cf['model']['ewm']['ci_level']
        )
        self.rate_caches = {i: pd.Series() for i in self.instruments}
        self.ewm_caches = {i: dict() for i in self.instruments}

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self.check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            for i in self.instruments:
                self.refresh_oanda_dict()
                df_new = self.fetch_cached_rates(instrument=i)
                if df_new is None:
                    self.logger.info('No updated data')
                else:
                    self._update_caches(instrument=i, df_new=df_new)
                    side = self._determine_order_side(instrument=i)
                    if side:
                        self.design_and_place_order(instrument=i, side=side)
                    if self.log_dir_path:
                        self.write_rate_log(instrument=i, df_new=df_new)

    def _update_caches(self, instrument, df_new):
        self.latest_update_time = datetime.now()
        self.logger.debug('df_new:{0}{1}'.format(os.linesep, df_new))
        mid = self.rate_caches[instrument].append(
            df_new['mid']
        ).tail(n=int(self.cf['model']['ewm']['window'][1]))
        self.rate_caches[instrument] = mid
        self.ewm_caches[instrument] = {
            **df_new.tail(n=1).reset_index().T.to_dict()[0],
            **self.ewmld.calculate_ci(series=mid)
        }

    def _determine_order_side(self, instrument):
        od = self.oanda_dict
        ec = self.ewm_caches[instrument]
        pos = [p for p in od['positions'] if p['instrument'] == instrument][0]
        tr = [d for d in od['instruments'] if d['instrument'] == instrument][0]
        mp = self.cf['model']['ewm']
        pp = self.cf['position']
        if self.rate_caches[instrument].size < mp['window'][0]:
            side = None
            state = 'LOADING'
        elif pos:
            side = None
            state = {'buy': 'LONG', 'sell': 'SHORT'}[pos['side']]
        elif tr['halted']:
            side = None
            state = 'TRADING HALTED'
        elif od['marginUsed'] > od['balance'] * pp['margin_nav_ratio']['max']:
            side = None
            state = 'LACK OF FUND'
        elif ec['spread'] > ec['mid'] * pp['limit_price_ratio']['max_spread']:
            side = None
            state = 'OVER-SPREAD'
        elif ec['ci'][0] > 0:
            side = 'buy'
            state = 'OPEN LONG'
        elif ec['ci'][1] < 0:
            side = 'sell'
            state = 'OPEN SHORT'
        else:
            side = None
            state = ''
        self.print_log(
            '| {0:7} | RATE: {1:>20} | LD: {2:>20} | {3:^14} |'.format(
                instrument,
                np.array2string(
                    np.array([ec['bid'], ec['ask']]),
                    formatter={'float_kind': lambda f: '{:8g}'.format(f)}
                ),
                np.array2string(
                    ec['ci'],
                    formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
                ),
                state
            )
        )
        return side


class EwmLogDiff(object):
    def __init__(self, alpha=0.01, ci_level=0.99, ewm_adjust=False):
        self.alpha = alpha
        self.ci_level = ci_level
        self.ewm_adjust = ewm_adjust

    def calculate_ci(self, series):
        ewm = np.log(series).diff().ewm(
            alpha=self.alpha, adjust=self.ewm_adjust, ignore_na=True
        )
        mu = ewm.mean().values[-1]
        sigma = ewm.std().values[-1]
        ci = np.array(
            stats.norm.interval(alpha=self.ci_level, loc=mu, scale=sigma)
        )
        return {'mean': mu, 'std': sigma, 'ci': ci}
