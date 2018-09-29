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
        self.ewm_stats = {i: dict() for i in self.instruments}

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.log_dir_path:
            self.write_parameter_log()
        while self.check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            self._open_deals()

    def _open_deals(self):
        for i in self.instruments:
            self.refresh_oanda_dict()
            df_new = self.fetch_cached_rates(instrument=i)
            if df_new is None:
                self.logger.info('No updated data')
            else:
                self._update_caches(instrument=i, df_new=df_new)
                st = self._determine_order_side(instrument=i)
                if st['side']:
                    self.design_and_place_order(instrument=i, side=st['side'])
                if self.log_dir_path:
                    self.write_df_log(df=df_new, name='rate.{}.csv'.format(i))
                    self.write_df_log(
                        df=pd.DataFrame([st]).set_index('time', drop=True),
                        name='ewm.{}.csv'.format(i)
                    )

    def _update_caches(self, instrument, df_new):
        self.latest_update_time = datetime.now()
        self.logger.debug('df_new:{0}{1}'.format(os.linesep, df_new))
        mid = self.rate_caches[instrument].append(
            df_new['mid']
        ).tail(n=int(self.cf['model']['ewm']['window'][1]))
        self.rate_caches[instrument] = mid
        self.ewm_stats[instrument] = {
            **df_new.tail(n=1).reset_index().T.to_dict()[0],
            **self.ewmld.calculate_ci(series=mid)
        }

    def _determine_order_side(self, instrument):
        od = self.oanda_dict
        ec = self.ewm_stats[instrument]
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
        elif ec['ewmacil'] > 0:
            side = 'buy'
            state = 'OPEN LONG'
        elif ec['ewmaciu'] < 0:
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
                    np.array([ec['ewmacil'], ec['ewmaciu']]),
                    formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
                ),
                state
            )
        )
        return {'side': side, 'state': state, **ec}


class EwmLogDiff(object):
    def __init__(self, alpha=0.01, ci_level=0.99, ewm_adjust=False):
        self.alpha = alpha
        self.ci_level = ci_level
        self.ewm_adjust = ewm_adjust

    def calculate_ci(self, series):
        ewm = np.log(series).diff().ewm(
            alpha=self.alpha, adjust=self.ewm_adjust, ignore_na=True
        )
        m = ewm.mean().values[-1]
        s = ewm.std().values[-1]
        ci = stats.norm.interval(alpha=self.ci_level, loc=m, scale=s)
        return {'ewma': m, 'ewmstd': s, 'ewmacil': ci[0], 'ewmaciu': ci[1]}
