#!/usr/bin/env python

from datetime import datetime
import os
from pprint import pformat
import signal
import numpy as np
import pandas as pd
from scipy import stats
from .kvs import RedisTrader


class EwmLogDiffTrader(RedisTrader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.alpha = self.cf['model']['ewm']['alpha']
        self.ci_level = self.cf['model']['ewm']['ci_level']
        self.rate_caches = {i: pd.Series() for i in self.instruments}
        self.ewm_stats = {i: dict() for i in self.instruments}
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.log_dir_path:
            self.write_parameter_log(dir_path=self.log_dir_path)
        while self.check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            self._open_deals()

    def _open_deals(self):
        for i in self.instruments:
            len_h = len(self.position_hists[i])
            self.refresh_oanda_dict()
            df_p = self.position_hists[i][len_h:]
            if df_p.size:
                self.logger.info('Position:{0}{1}'.format(os.linesep, df_p))
            else:
                self.logger.info('No updated position')
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
                self.logger.info('No updated rate')
                df_s = pd.DataFrame()
            log_paths = {
                os.path.join(self.log_dir_path, '{0}.{1}.tsv'.format(k, i)): v
                for k, v in {'pos': df_p, 'rate': df_r, 'stat': df_s}.items()
                if self.log_dir_path and v.size
            }
            for p, d in log_paths.items():
                self.write_df_log(df=d, path=p)
                self.logger.info('Updated TSV log: {}'.format(p))

    def _update_caches(self, instrument, df_rate):
        self.latest_update_time = datetime.now()
        rate = df_rate.tail(n=1).reset_index().T.to_dict()[0]
        mid = self.rate_caches[instrument].append(
            (df_rate['bid'] + df_rate['ask']) / 2
        ).tail(n=int(self.cf['model']['ewm']['window'][1])).astype(np.float32)
        self.rate_caches[instrument] = mid
        ewm = np.log(mid).diff().ewm(alpha=self.alpha, adjust=True)
        self.logger.debug('ewm: {}'.format(ewm))
        mu = ewm.mean().values[-1]
        sigma = ewm.std().values[-1]
        ci = stats.norm.interval(alpha=self.ci_level, loc=mu, scale=sigma)
        self.logger.debug('ewma ci: {0} [{1} {2}]'.format(mu, *ci))
        self.ewm_stats[instrument] = {
            'ewma': mu, 'ewmstd': sigma, 'ewmacil': ci[0], 'ewmaciu': ci[1],
            'spread_ratio': np.float16((rate['ask'] - rate['bid']) / mid[-1]),
            'mid': mid[-1], **rate
        }

    def _determine_order_side(self, instrument):
        od = self.oanda_dict
        ec = self.ewm_stats[instrument]
        mp = self.cf['model']['ewm']
        pp = self.cf['position']
        tr = {d['instrument']: d for d in od['instruments']}.get(instrument)
        pos = {d['instrument']: d for d in od['positions']}.get(instrument)
        preserved_margin = od['balance'] * pp['margin_nav_ratio']['preserve']
        if self.rate_caches[instrument].size < mp['window'][0]:
            st = {'act': None, 'state': 'LOADING'}
        elif tr['halted']:
            st = {'act': None, 'state': 'TRADING HALTED'}
        elif od['balance'] == 0:
            st = {'act': None, 'state': 'NO FUND'}
        elif od['marginAvail'] < preserved_margin and not pos:
            st = {'act': None, 'state': 'LACK OF FUNDS'}
        elif ec['spread_ratio'] > pp['limit_price_ratio']['max_spread']:
            st = {'act': None, 'state': 'OVER-SPREAD'}
        elif ec['ewmacil'] > 0:
            if pos and pos['side'] == 'buy':
                st = {'act': None, 'state': 'LONG'}
            elif pos and pos['side'] == 'sell':
                st = {'act': 'buy', 'state': 'SHORT >>> LONG'}
            else:
                st = {'act': 'buy', 'state': '>>> LONG'}
        elif ec['ewmaciu'] < 0:
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
            (
                '|{0:^11}| RATE:{1:>21} | LOGDIFF CI{2:02d}:{3:>20} |{4:^16}|'
            ).format(
                instrument,
                np.array2string(
                    np.array([ec['bid'], ec['ask']]),
                    formatter={'float_kind': lambda f: '{:8g}'.format(f)}
                ),
                int(self.ci_level * 100),
                np.array2string(
                    np.array([ec['ewmacil'], ec['ewmaciu']]),
                    formatter={'float_kind': lambda f: '{:1.5f}'.format(f)}
                ),
                st['state']
            )
        )
        return {**st, **ec}
