#!/usr/bin/env python

import logging
import os
import signal
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime
from pprint import pformat

import numpy as np
import pandas as pd
import yaml

import ujson
from oandacli.util.config import create_api, log_response
from v20 import V20ConnectionError, V20Timeout

from ..util.error import APIResponseError
from .bet import BettingSystem
from .ewma import Ewma


class TraderCore(object):
    def __init__(self, config_dict, instruments, log_dir_path=None,
                 quiet=False, dry_run=False):
        self.__logger = logging.getLogger(__name__)
        self.cf = config_dict
        self.__api = create_api(config=self.cf)
        self.__account_id = self.cf['oanda']['account_id']
        self.instruments = (instruments or self.cf['instruments'])
        self.__bs = BettingSystem(strategy=self.cf['position']['bet'])
        self.__quiet = quiet
        self.__dry_run = dry_run
        if log_dir_path:
            self.__log_dir_path = os.path.abspath(
                os.path.expanduser(os.path.expandvars(log_dir_path))
            )
            os.makedirs(self.__log_dir_path, exist_ok=True)
            self.__order_log_path = os.path.join(
                self.__log_dir_path, 'order.json.txt'
            )
            self.__txn_log_path = os.path.join(
                self.__log_dir_path, 'txn.json.txt'
            )
            self._write_data(
                yaml.dump(
                    {
                        'instrument': self.instruments,
                        'position': self.cf['position'],
                        'feature': self.cf['feature'],
                        'model': self.cf['model']
                    },
                    default_flow_style=False
                ).strip(),
                path=os.path.join(self.__log_dir_path, 'parameter.yml'),
                mode='w', append_linesep=False
            )
        else:
            self.__log_dir_path = None
            self.__order_log_path = None
            self.__txn_log_path = None
        self.__last_txn_id = None
        self.pos_dict = dict()
        self.balance = None
        self.margin_avail = None
        self.__account_currency = None
        self.__txn_list = list()
        self.__inst_dict = dict()
        self.price_dict = dict()
        self.unit_costs = dict()

    def _refresh_account_dicts(self):
        res = self.__api.account.get(accountID=self.__account_id)
        log_response(res, logger=self.__logger)
        if 'account' in res.body:
            acc = res.body['account']
        else:
            raise APIResponseError(
                'unexpected response:' + os.linesep + pformat(res.body)
            )
        self.balance = float(acc.balance)
        self.margin_avail = float(acc.marginAvailable)
        self.__account_currency = acc.currency
        pos_dict0 = self.pos_dict
        self.pos_dict = {
            p.instrument: (
                {'side': 'long', 'units': int(p.long.units)} if p.long.tradeIDs
                else {'side': 'short', 'units': int(p.short.units)}
            ) for p in acc.positions if p.long.tradeIDs or p.short.tradeIDs
        }
        for i, d in self.pos_dict.items():
            p0 = pos_dict0.get(i)
            if p0 and all([p0[k] == d[k] for k in ['side', 'units']]):
                self.pos_dict[i]['dt'] = p0['dt']
            else:
                self.pos_dict[i]['dt'] = datetime.now()

    def expire_positions(self, ttl_sec=86400):
        for i, d in self.pos_dict.items():
            es = (datetime.now() - d['dt']).total_seconds()
            self.__logger.info(
                '{0}: {1} => {2} sec elapsed'.format(d['units'], d['side'], es)
            )
            if es > ttl_sec:
                self.__logger.info('Close a position: {}'.format(d['side']))
                self._place_order(closing=True, instrument=i)

    def _place_order(self, closing=False, **kwargs):
        if closing:
            p = self.pos_dict.get(kwargs['instrument'])
            f_args = {
                'accountID': self.__account_id, **kwargs,
                **{
                    '{}Units'.format(k):
                    ('ALL' if p and p['side'] == k else 'NONE')
                    for k in ['long', 'short']
                }
            }
        else:
            f_args = {'accountID': self.__account_id, **kwargs}
        if self.__dry_run:
            self.__logger.info(
                os.linesep + pformat({
                    'func': ('position.close' if closing else 'order.create'),
                    'args': f_args
                })
            )
        else:
            if closing:
                res = self.__api.position.close(**f_args)
            else:
                res = self.__api.order.create(**f_args)
            log_response(res, logger=self.__logger)
            if not (100 <= res.status <= 399):
                raise APIResponseError(
                    'unexpected response:' + os.linesep + pformat(res.body)
                )
            elif self.__order_log_path:
                self._write_data(
                    ujson.dumps(res.body), path=self.__order_log_path
                )
            else:
                time.sleep(0.5)

    def refresh_oanda_dicts(self):
        t0 = datetime.now()
        self._refresh_account_dicts()
        self._sleep(last=t0, sec=0.5)
        self._refresh_txn_list()
        self._sleep(last=t0, sec=1)
        self._refresh_inst_dict()
        self._sleep(last=t0, sec=1.5)
        self._refresh_price_dict()
        self._refresh_unit_costs()

    def _refresh_txn_list(self):
        res = (
            self.__api.transaction.since(
                accountID=self.__account_id, id=self.__last_txn_id
            ) if self.__last_txn_id
            else self.__api.transaction.list(accountID=self.__account_id)
        )
        log_response(res, logger=self.__logger)
        if 'lastTransactionID' in res.body:
            self.__last_txn_id = res.body['lastTransactionID']
        else:
            raise APIResponseError(
                'unexpected response:' + os.linesep + pformat(res.body)
            )
        if res.body.get('transactions'):
            t_new = [vars(t) for t in res.body['transactions']]
            self.print_log(yaml.dump(t_new, default_flow_style=False).strip())
            self.__txn_list = self.__txn_list + t_new
            if self.__txn_log_path:
                self._write_data(ujson.dumps(t_new), path=self.__txn_log_path)

    def _refresh_inst_dict(self):
        res = self.__api.account.instruments(accountID=self.__account_id)
        log_response(res, logger=self.__logger)
        if 'instruments' in res.body:
            self.__inst_dict = {
                c.name: vars(c) for c in res.body['instruments']
            }
        else:
            raise APIResponseError(
                'unexpected response:' + os.linesep + pformat(res.body)
            )

    def _refresh_price_dict(self):
        res = self.__api.pricing.get(
            accountID=self.__account_id,
            instruments=','.join(self.__inst_dict.keys())
        )
        log_response(res, logger=self.__logger)
        if 'prices' in res.body:
            self.price_dict = {
                p.instrument: {
                    'bid': p.closeoutBid, 'ask': p.closeoutAsk,
                    'tradeable': p.tradeable
                } for p in res.body['prices']
            }
        else:
            raise APIResponseError(
                'unexpected response:' + os.linesep + pformat(res.body)
            )

    def _refresh_unit_costs(self):
        self.unit_costs = {
            i: self._calculate_bp_value(instrument=i) * float(e['marginRate'])
            for i, e in self.__inst_dict.items() if i in self.instruments
        }

    def _calculate_bp_value(self, instrument):
        cur_pair = instrument.split('_')
        if cur_pair[0] == self.__account_currency:
            bpv = 1 / self.price_dict[instrument]['ask']
        elif cur_pair[1] == self.__account_currency:
            bpv = self.price_dict[instrument]['ask']
        else:
            inst_bpv = [
                i for i in self.__inst_dict.keys()
                if set(i.split('_')) == {cur_pair[1], self.__account_currency}
            ][0]
            bpv = self.price_dict[instrument]['ask'] * (
                self.price_dict[inst_bpv]['ask']
                if inst_bpv.split('_')[1] == self.__account_currency
                else (1 / self.price_dict[inst_bpv]['ask'])
            )
        return bpv

    def design_and_place_order(self, instrument, act):
        pos = self.pos_dict.get(instrument)
        if act and pos and (act == 'closing' or act != pos['side']):
            self.__logger.info('Close a position: {}'.format(pos['side']))
            self._place_order(closing=True, instrument=instrument)
            self._refresh_txn_list()
        if act in ['long', 'short']:
            limits = self._design_order_limits(instrument=instrument, side=act)
            self.__logger.debug('limits: {}'.format(limits))
            units = self._design_order_units(instrument=instrument, side=act)
            self.__logger.debug('units: {}'.format(units))
            self.__logger.info('Open a order: {}'.format(act))
            self._place_order(
                order={
                    'type': 'MARKET', 'instrument': instrument, 'units': units,
                    'timeInForce': 'FOK', 'positionFill': 'DEFAULT', **limits
                }
            )

    def _design_order_limits(self, instrument, side):
        ie = self.__inst_dict[instrument]
        r = self.price_dict[instrument][{'long': 'ask', 'short': 'bid'}[side]]
        ts_range = [
            float(ie['minimumTrailingStopDistance']),
            float(ie['maximumTrailingStopDistance'])
        ]
        ts_dist_ratio = int(
            r * self.cf['position']['limit_price_ratio']['trailing_stop'] /
            ts_range[0]
        )
        if ts_dist_ratio <= 1:
            trailing_stop = ie['minimumTrailingStopDistance']
        else:
            ts_dist = np.float16(ts_range[0] * ts_dist_ratio)
            if ts_dist >= ts_range[1]:
                trailing_stop = ie['maximumTrailingStopDistance']
            else:
                trailing_stop = str(ts_dist)
        tp = {
            k: str(
                np.float16(
                    r + r * v * {
                        'take_profit': {'long': 1, 'short': -1}[side],
                        'stop_loss': {'long': -1, 'short': 1}[side]
                    }[k]
                )
            ) for k, v in self.cf['position']['limit_price_ratio'].items()
            if k in ['take_profit', 'stop_loss']
        }
        tif = {'timeInForce': 'GTC'}
        return {
            'takeProfitOnFill': {'price': tp['take_profit'], **tif},
            'stopLossOnFill': {'price': tp['stop_loss'], **tif},
            'trailingStopLossOnFill': {'distance': trailing_stop, **tif}
        }

    def _design_order_units(self, instrument, side):
        max_size = int(self.__inst_dict[instrument]['maximumOrderUnits'])
        avail_size = max(
            np.ceil(
                (
                    self.margin_avail - self.balance *
                    self.cf['position']['margin_nav_ratio']['preserve']
                ) / self.unit_costs[instrument]
            ), 0
        )
        self.__logger.debug('avail_size: {}'.format(avail_size))
        sizes = {
            k: np.ceil(self.balance * v / self.unit_costs[instrument])
            for k, v in self.cf['position']['margin_nav_ratio'].items()
            if k in ['unit', 'init']
        }
        self.__logger.debug('sizes: {}'.format(sizes))
        bet_size = self.__bs.calculate_size_by_pl(
            unit_size=sizes['unit'],
            inst_pl_txns=[
                t for t in self.__txn_list if (
                    t.get('instrument') == instrument and t.get('pl') and
                    t.get('units')
                )
            ],
            init_size=sizes['init']
        )
        self.__logger.debug('bet_size: {}'.format(bet_size))
        return str(
            int(min(bet_size, avail_size, max_size)) *
            {'long': 1, 'short': -1}[side]
        )

    @staticmethod
    def _sleep(last, sec=0.5):
        rest = sec - (datetime.now() - last).total_seconds()
        if rest > 0:
            time.sleep(rest)

    def print_log(self, data):
        if self.__quiet:
            self.__logger.info(data)
        else:
            print(data, flush=True)

    def print_state_line(self, df_rate, add_str):
        i = df_rate['instrument'].iloc[-1]
        net_pl = sum([
            float(t['pl']) for t in self.__txn_list
            if t.get('instrument') == i and t.get('pl')
        ])
        self.print_log(
            '|{0:^11}|{1:^29}|{2:^13}|'.format(
                i,
                '{0:>3}:{1:>21}'.format(
                    'B/A',
                    np.array2string(
                        df_rate[['bid', 'ask']].iloc[-1].values,
                        formatter={'float_kind': lambda f: '{:8g}'.format(f)}
                    )
                ),
                'PL:{:>6}'.format(int(net_pl))
            ) + (add_str or '')
        )

    def _write_data(self, data, path, mode='a', append_linesep=True):
        with open(path, mode) as f:
            f.write(str(data) + (os.linesep if append_linesep else ''))

    def write_turn_log(self, df_rate, **kwargs):
        i = df_rate['instrument'].iloc[-1]
        df_r = df_rate.drop(columns=['instrument'])
        self._write_log_df(name='rate.{}'.format(i), df=df_r)
        if kwargs:
            self._write_log_df(
                name='sig.{}'.format(i), df=df_r.tail(n=1).assign(**kwargs)
            )

    def _write_log_df(self, name, df):
        if self.__log_dir_path and df.size:
            self.__logger.debug('{0} df:{1}{2}'.format(name, os.linesep, df))
            p = os.path.join(self.__log_dir_path, '{}.tsv'.format(name))
            self.__logger.info('Write TSV log: {}'.format(p))
            self._write_df(df=df, path=p)

    def _write_df(self, df, path, mode='a'):
        df.to_csv(
            path, mode=mode, sep=(',' if path.endswith('.csv') else '\t'),
            header=(not os.path.isfile(path))
        )

    def fetch_candle_df(self, instrument, granularity='S5', count=5000):
        res = self.__api.instrument.candles(
            instrument=instrument, price='BA', granularity=granularity,
            count=int(count)
        )
        log_response(res, logger=self.__logger)
        if 'candles' in res.body:
            return pd.DataFrame([
                {
                    'time': c.time, 'bid': c.bid.c, 'ask': c.ask.c,
                    'volume': c.volume
                } for c in res.body['candles'] if c.complete
            ]).assign(
                time=lambda d: pd.to_datetime(d['time']), instrument=instrument
            ).set_index('time', drop=True)
        else:
            raise APIResponseError(
                'unexpected response:' + os.linesep + pformat(res.body)
            )

    def fetch_latest_price_df(self, instrument):
        res = self.__api.pricing.get(
            accountID=self.__account_id, instruments=instrument
        )
        log_response(res, logger=self.__logger)
        if 'prices' in res.body:
            return pd.DataFrame([
                {'time': r.time, 'bid': r.closeoutBid, 'ask': r.closeoutAsk}
                for r in res.body['prices']
            ]).assign(
                time=lambda d: pd.to_datetime(d['time']), instrument=instrument
            ).set_index('time')
        else:
            raise APIResponseError(
                'unexpected response:' + os.linesep + pformat(res.body)
            )


class BaseTrader(TraderCore, metaclass=ABCMeta):
    def __init__(self, model, standalone=True, ignore_api_error=False,
                 **kwargs):
        super().__init__(**kwargs)
        self.__logger = logging.getLogger(__name__)
        self.__ignore_api_error = ignore_api_error
        self.__n_cache = self.cf['feature']['cache_length']
        self.__use_tick = (
            'TICK' in self.cf['feature']['granularities'] and not standalone
        )
        self.__granularities = [
            a for a in self.cf['feature']['granularities'] if a != 'TICK'
        ]
        self.__cache_dfs = {i: pd.DataFrame() for i in self.instruments}
        if model == 'ewma':
            self.__ai = Ewma(config_dict=self.cf)
        else:
            raise ValueError('invalid model name: {}'.format(model))

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self.check_health():
            try:
                self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
                for i in self.instruments:
                    self.refresh_oanda_dicts()
                    self.make_decision(instrument=i)
            except (V20ConnectionError, V20Timeout, APIResponseError) as e:
                if self.__ignore_api_error:
                    self.__logger.error(e)
                else:
                    raise e

    @abstractmethod
    def check_health(self):
        return True

    @abstractmethod
    def make_decision(self, instrument):
        pass

    def update_caches(self, df_rate):
        self.__logger.info('Rate:{0}{1}'.format(os.linesep, df_rate))
        i = df_rate['instrument'].iloc[-1]
        df_c = self.__cache_dfs[i].append(df_rate).tail(n=self.__n_cache)
        self.__logger.info('Cache length: {}'.format(len(df_c)))
        self.__cache_dfs[i] = df_c

    def determine_sig_state(self, df_rate):
        i = df_rate['instrument'].iloc[-1]
        pos = self.pos_dict.get(i)
        pos_pct = int(
            (abs(pos['units']) * self.unit_costs[i] * 100 / self.balance)
            if pos else 0
        )
        sig = self.__ai.detect_signal(
            history_dict=self._fetch_history_dict(instrument=i), pos=pos
        )
        if not self.price_dict[i]['tradeable']:
            act = None
            state = 'TRADING HALTED'
        elif sig['sig_act'] == 'closing':
            act = 'closing'
            state = 'CLOSING'
        elif int(self.balance) == 0:
            act = None
            state = 'NO FUND'
        elif self._is_margin_lack(instrument=i):
            act = None
            state = 'LACK OF FUNDS'
        elif self._is_over_spread(df_rate=df_rate):
            act = None
            state = 'OVER-SPREAD'
        elif sig['sig_act'] == 'long':
            if pos and pos['side'] == 'long':
                act = None
                state = '{:.1f}% LONG'.format(pos_pct)
            elif pos and pos['side'] == 'short':
                act = 'long'
                state = 'SHORT -> LONG'
            else:
                act = 'long'
                state = '-> LONG'
        elif sig['sig_act'] == 'short':
            if pos and pos['side'] == 'short':
                act = None
                state = '{:.1f}% SHORT'.format(pos_pct)
            elif pos and pos['side'] == 'long':
                act = 'short'
                state = 'LONG -> SHORT'
            else:
                act = 'short'
                state = '-> SHORT'
        elif pos and pos['side'] == 'long':
            act = None
            state = '{:.1f}% LONG'.format(pos_pct)
        elif pos and pos['side'] == 'short':
            act = None
            state = '{:.1f}% SHORT'.format(pos_pct)
        else:
            act = None
            state = '-'
        log_str = (
            (
                '{:^14}|'.format('TICK:{:>5}'.format(len(df_rate)))
                if self.__use_tick else ''
            ) + sig['sig_log_str'] + '{:^18}|'.format(state)
        )
        return {'act': act, 'state': state, 'log_str': log_str, **sig}

    def _fetch_history_dict(self, instrument):
        df_c = self.__cache_dfs[instrument]
        return {
            **(
                {'TICK': df_c.assign(volume=1)}
                if self.__use_tick and len(df_c) == self.__n_cache else dict()
            ),
            **{
                g: self.fetch_candle_df(
                    instrument=instrument, granularity=g, count=self.__n_cache
                ).rename(
                    columns={'closeAsk': 'ask', 'closeBid': 'bid'}
                )[['ask', 'bid', 'volume']] for g in self.__granularities
            }
        }

    def _is_margin_lack(self, instrument):
        return (
            not self.pos_dict.get(instrument) and
            self.margin_avail <=
            self.balance * self.cf['position']['margin_nav_ratio']['preserve']
        )

    def _is_over_spread(self, df_rate):
        return (
            df_rate.tail(n=1).pipe(
                lambda d: (d['ask'] - d['bid']) / (d['ask'] + d['bid']) * 2
            ).values[0] >=
            self.cf['position']['limit_price_ratio']['max_spread']
        )
