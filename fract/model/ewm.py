#!/usr/bin/env python

from datetime import datetime
import json
import logging
from math import floor
import os
from pprint import pformat
import signal
import time
import numpy as np
import oandapy
import pandas as pd
import redis
from scipy import stats
from ..cli.util import FractError


class FractRedisTrader(oandapy.API):
    def __init__(self, config_dict, instruments, redis_host='127.0.0.1',
                 redis_port=6379, redis_db=0, redis_pool=None, interval_sec=1,
                 log_dir_path=None, quiet=False):
        self.logger = logging.getLogger(__name__)
        self.cf = config_dict
        super().__init__(
            environment=config_dict['oanda']['environment'],
            access_token=config_dict['oanda']['access_token']
        )
        self.account_id = config_dict['oanda']['account_id'],
        self.instruments = instruments or config_dict['instruments']
        self.interval_sec = int(interval_sec)
        self.log_dir_path = os.path.abspath(os.path.expanduser(log_dir_path))
        self.quiet = quiet
        self.redis_pool = redis_pool or redis.ConnectionPool(
            host=redis_host, port=int(redis_port), db=int(redis_db)
        )
        self.is_active = True
        self.oanda_dict = dict()
        self.history_books = {i: pd.DataFrame() for i in self.instruments}
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def close_positions(self, instruments=[]):
        return [
            self.close_position(account_id=self.account_id, instrument=i)
            for i in {
                p['instrument'] for p in
                self.get_positions(account_id=self.account_id)['positions']
            } if not instruments or i in instruments
        ]

    def refresh_oanda_dict(self):
        account = self.get_account(account_id=self.account_id)
        time.sleep(0.5)
        instruments = self.get_instruments(
            account_id=self.account_id,
            fields=','.join([
                'displayName', 'pip', 'maxTradeUnits', 'precision',
                'maxTrailingStop', 'minTrailingStop', 'marginRate', 'halted'
            ])
        )
        time.sleep(0.5)
        prices = self.get_prices(
            account_id=self.account_id,
            instruments=','.join([
                d['instrument'] for d in instruments['instruments']
            ])
        )
        time.sleep(0.5)
        positions = self.get_positions(account_id=self.account_id)
        self.oanda_dict = {**account, **instruments, **prices, **positions}
        self.logger.debug('self.oanda_dict: ' + pformat(self.oanda_dict))
        for i in self.instruments:
            df_p = pd.DataFrame([p for p in positions if p['instrument'] == i])
            df_r = pd.DataFrame(
                [r for r in prices if r['instrument'] == i]
            )[['time', 'bid', 'ask']]
            df_h = self.history_books[i].tail(n=1)
            if df_h.size and df_h['units'].values[0] and not df_p.size:
                self.history_books[i].append(
                    pd.concat([df_h, df_r], axis=1).assign(
                        time=lambda d: pd.to_datetime(d['time']),
                        pl=lambda d: (
                            d['bid'] - d['avgPrice']
                            if df_h['side'].values[0] == 'buy' else
                            d['avgPrice'] - d['ask']
                        ) * d['units']
                    )
                )
            elif df_p.size and not (df_h.size and df_h['units'].values[0]):
                self.history_books[i].append(
                    pd.concat([df_p, df_r], axis=1).assign(
                        time=lambda d: pd.to_datetime(d['time']), pl=np.nan
                    )
                )

    def expire_positions(self, ttl_sec=86400):
        prices = self.get_prices(
            account_id=self.account_id, instruments=','.join(self.instruments)
        )
        time.sleep(0.5)
        for p in self.get_positions(account_id=self.account_id):
            i = p['instrument']
            df_h = self.history_books[i].tail(n=1)
            if df_h.size and df_h['pl'].values[0] is np.nan:
                t = [r for r in prices if r['instrument'] == i][0]['time']
                td = (pd.to_datetime(t) - df_h['time']).value[0]
                if td > np.timedelta64(ttl_sec, 's'):
                    r = self.close_position(
                        account_id=self.account_id, instrument=p['instrument']
                    )
                    self.logger.info(
                        'Close an expired position:' + os.linesep + pformat(r)
                    )
                    if self.log_dir_path:
                        self.write_order_log(response=r)
                    else:
                        time.sleep(0.5)

    def fetch_cached_rates(self, instrument):
        redis_c = redis.StrictRedis(connection_pool=self.redis_pool)
        cached_rates = [
            json.loads(s) for s in redis_c.lrange(instrument, 0, -1)
        ]
        if cached_rates:
            for d in cached_rates:
                redis_c.lpop(instrument)
            if [r for r in cached_rates if 'disconnect' in r]:
                self.logger.warning(
                    'cached_rates:' + os.linesep + pformat(cached_rates)
                )
                self.is_active = False
                return None
            else:
                self.logger.debug('cached_rates: ' + pformat(cached_rates))
                return pd.DataFrame(
                    [d['tick'] for d in cached_rates if 'tick' in d]
                ).assign(
                    time=lambda d: pd.to_datetime(d['time']),
                    mid=lambda d: (d['ask'] + d['bid']) / 2,
                    spread=lambda d: d['ask'] - d['bid']
                ).set_index('time', drop=True)
        else:
            return None

    def calculate_bp_value(self, instrument):
        prices = {p['instrument']: p for p in self.oanda_dict['prices']}
        cur_pair = instrument.split('_')
        if cur_pair[0] == self.account_currency:
            bpv = 1 / prices[instrument]['ask']
        elif cur_pair[1] == self.account_currency:
            bpv = prices[instrument]['ask']
        else:
            inst_bpv = [
                i['instrument'] for i in self.oanda_dict['instruments']
                if set(i['instrument'].split('_')) == {
                    cur_pair[1], self.account_currency
                }
            ][0]
            if inst_bpv.split('_')[1] == self.account_currency:
                bpv = prices[instrument]['ask'] * prices[inst_bpv]['ask']
            else:
                bpv = prices[instrument]['ask'] / prices[inst_bpv]['ask']
        return bpv

    def calculate_order_limits(self, instrument, side):
        inst_dict = [
            i for i in self.oanda_dict['instruments'] if i == instrument
        ][0]
        open_price = [
            p[{'buy': 'ask', 'sell': 'bid'}[side]]
            for p in self.oanda_dict['prices'] if p['instrument'] == instrument
        ][0]
        trailing_stop = min([
            max([
                int(
                    self.cf['position']['limit_price_ratio']['trailing_stop'] *
                    open_price / np.float32(inst_dict['pip'])
                ),
                inst_dict['minTrailingStop']
            ]),
            inst_dict['maxTrailingStop']
        ])
        tp = {
            k: np.float16(
                1 + v * {
                    'take_profit': {'buy': 1, 'sell': -1}[side],
                    'stop_loss': {'buy': -1, 'sell': 1}[side],
                    'upper_bound': 1, 'lower_bound': -1
                }[k]
            ) * open_price
            for k, v in self.cf['position']['limit_price_ratio']
            if k in ['take_profit', 'stop_loss', 'upper_bound', 'lower_bound']
        }
        return {
            'trailingStop': trailing_stop, 'takeProfit': tp['take_profit'],
            'stopLoss': tp['stop_loss'], 'upperBound': tp['upper_bound'],
            'lowerBound': tp['lower_bound']
        }

    def place_order(self, **kwargs):
        try:
            r = self.create_order(**kwargs)
        except Exception as e:
            self.logger.error(pformat(r))
            self.logger.error(e)
        else:
            self.print_log('Open on order:' + os.linesep + pformat(r))
        finally:
            if self.log_dir_path:
                self.write_order_log(response=r)

    def print_log(self, data):
        if self.quiet:
            self.logger.info(data)
        else:
            print(data)

    def write_rate_log(self, instrument, df_new):
        p = os.path.join(
            self.log_dir_path, 'rate_{}.log.csv'.format(instrument)
        )
        df_new.to_csv(p, mode='a', header=(not os.path.isfile(p)))

    def write_order_log(self, response):
        p = os.path.join(self.log_dir_path, 'order.log.csv')
        with open(p, 'a') as f:
            f.write(json.dumps(response))


class EwmLogDiffTrader(FractRedisTrader):
    def __init__(self, timeout_sec=3600, **kwargs):
        super().__init__(**kwargs)
        self.timeout_sec = int(timeout_sec)
        self.ewmld = EwmLogDiff(
            alpha=self.cf['model']['alpha'],
            ci_level=self.cf['model']['ci_level']
        )
        self.bs = BettingSystem(strategy=self.cf['position']['bet'])
        self.rate_caches = {i: pd.Series() for i in self.instruments}
        self.ewm_caches = {i: dict() for i in self.instruments}
        self.latest_update_time = None

    def invoke(self):
        self.close_positions(instruments=self.instruments)
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self._check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            for i in self.instruments:
                self.refresh_oanda_dict()
                self._trade(instrument=i)

    def _check_health(self):
        if self.latest_update_time is None and self.is_active:
            return True
        else:
            td = datetime.now() - self.latest_update_time
            if td.total_seconds() > self.timeout_sec:
                self.logger.warning('Timeout due to no data update')
                self.redis_pool.disconnect()
                return False
            elif self.is_active:
                time.sleep(secs=self.interval_sec)
                return True
            else:
                self.redis_pool.disconnect()
                return False

    def _trade(self, instrument):
        df_new = self.fetch_cached_rates(instrument=instrument)
        if df_new is None:
            self.logger.debug('No updated data')
        else:
            self.latest_update_time = datetime.now()
            self._update_caches(instrument=instrument, df_new=df_new)
            side = self._determine_order_side(instrument=instrument)
            if side:
                self._design_and_place_order(instrument=instrument, side=side)
            if self.log_dir_path:
                self.write_rate_log(instrument=instrument, df_new=df_new)

    def _update_caches(self, instrument, df_new):
        mid = self.rate_caches[instrument].append(
            df_new['mid']
        ).tail(n=self.cf['model']['ewm']['window'][1])
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
        pp = self.cf['position']
        if self.rate_caches[instrument].size < self.cf['model']['window'][0]:
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

    def _design_and_place_order(self, instrument, side):
        units = self._calculate_order_units(instrument=instrument, side=side)
        limits = self.calculate_order_limits(instrument=instrument, side=side)
        self.place_order(
            account_id=self.account_id, type='market', instrument=instrument,
            side=side, units=units, **limits
        )

    def _calculate_order_units(self, instrument, side):
        margin_per_bp = self.calculate_bp_value(instrument=instrument) * [
            i for i in self.oanda_dict['instruments'] if i == instrument
        ][0]['marginRate']
        avail_size = floor(
            (
                self.oanda_dict['marginAvail'] - self.oanda_dict['balance'] *
                self.cf['position']['margin_nav_ratio']['preserve']
            ) / margin_per_bp
        )
        sizes = {
            k: floor(self.oanda_dict['balance'] * v / margin_per_bp)
            for k, v in self.cf['position']['margin_nav_ratio']
        }
        if self.history_books[instrument].size:
            df_pl = self.history_books[instrument].dropna(subset=['pl'])
            bet_size = self.bs.calculate_size(
                unit_size=sizes['unit'], init_size=sizes['init'],
                last_size=df_pl['units'].values[-1],
                last_won=(df_pl['pl'].values[-1] > 0),
                is_all_time_high=df_pl['pl'].cumsum().pipe(
                    lambda s: s == max(s)
                ).values[-1]
            )
        else:
            bet_size = sizes['init']
        return min([bet_size, avail_size])


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


class BettingSystem(object):
    def __init__(self, strategy, init_to_unit_ratio=1):
        strategies = ['Martingale', "d'Alembert", 'Pyramid', "Oscar's grind"]
        if strategy in strategies:
            self.strategy = strategy
        else:
            raise FractError('invalid strategy name')

    def calculate_size(self, unit_size, init_size=None, last_size=None,
                       last_won=None, is_all_time_high=False):
        if last_size is None:
            return init_size or unit_size
        elif self.strategy == 'Martingale':
            return (unit_size if last_won else last_size * 2)
        elif self.strategy == "d'Alembert":
            return (unit_size if last_won else last_size + unit_size)
        elif self.strategy == 'Pyramid':
            if not last_won:
                return (last_size + unit_size)
            elif last_size < unit_size:
                return last_size
            else:
                return (last_size - unit_size)
        elif self.strategy == "Oscar's grind":
            if is_all_time_high:
                return init_size or unit_size
            elif last_won:
                return (last_size + unit_size)
            else:
                return last_size
        else:
            raise FractError('invalid strategy name')
