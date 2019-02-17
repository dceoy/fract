#!/usr/bin/env python

from abc import ABCMeta, abstractmethod
from datetime import datetime
import json
import logging
import os
from pprint import pformat
import signal
import time
import numpy as np
import oandapy
import pandas as pd
import yaml
from ..util.error import FractRuntimeError
from .bet import BettingSystem
from .ewma import Ewma


class TraderCoreAPI(oandapy.API):
    def __init__(self, config_dict, instruments, log_dir_path=None,
                 quiet=False, dry_run=False):
        self.__logger = logging.getLogger(__name__)
        self.cf = config_dict
        super().__init__(
            environment=self.cf['oanda']['environment'],
            access_token=self.cf['oanda']['access_token']
        )
        self.__account_id = self.cf['oanda']['account_id']
        self.instruments = (instruments or self.cf['instruments'])
        self.__bs = BettingSystem(strategy=self.cf['position']['bet'])
        self.__quiet = quiet
        self.__dry_run = dry_run
        self.__min_txn_id = max(
            [
                t['id'] for t in self.get_transaction_history(
                    account_id=self.__account_id
                )['transactions']
            ] + [0]
        ) + 1
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
        self.acc_dict = dict()
        self.__txn_list = list()
        self.inst_dict = dict()
        self.__rate_dict = dict()
        self.unit_costs = dict()
        self.pos_dict = dict()
        self.__pos_time = dict()

    def expire_positions(self, ttl_sec=86400):
        for i, p in self.pos_dict.items():
            if self.__pos_time.get(i):
                et_sec = (datetime.now() - self.__pos_time[i]).total_seconds()
                self.__logger.info('{0} => {1} sec elapsed'.format(p, et_sec))
                if et_sec > ttl_sec:
                    self.__logger.info(
                        'Close a position: {}'.format(p['side'])
                    )
                    self._place_order(closing=True, instrument=i)

    def refresh_oanda_dicts(self):
        t0 = datetime.now()
        self.acc_dict = self.get_account(account_id=self.__account_id)
        self._sleep(last=t0, sec=0.5)
        self._refresh_txn_list()
        self._sleep(last=t0, sec=1)
        self._refresh_inst_dict()
        self._sleep(last=t0, sec=1.5)
        self._refresh_rate_dict()
        self._refresh_unit_costs()
        self._sleep(last=t0, sec=2)
        self._refresh_pos_dict_and_pos_time()

    def _refresh_txn_list(self):
        th_new = [
            t for t in self.get_transaction_history(
                account_id=self.__account_id, minId=self.__min_txn_id
            ).get('transactions')
            if t['id'] not in [t['id'] for t in self.__txn_list]
        ]
        if th_new:
            self.print_log(yaml.dump(th_new, default_flow_style=False).strip())
            self.__txn_list = self.__txn_list + th_new
            if self.__txn_log_path:
                self._write_data(json.dumps(th_new), path=self.__txn_log_path)

    def _refresh_inst_dict(self):
        self.inst_dict = {
            d['instrument']: d
            for d in self.get_instruments(
                account_id=self.__account_id,
                fields=','.join([
                    'displayName', 'pip', 'maxTradeUnits', 'precision',
                    'maxTrailingStop', 'minTrailingStop', 'marginRate',
                    'halted'
                ])
            )['instruments'] if 'instrument' in d
        }

    def _refresh_rate_dict(self):
        self.__rate_dict = {
            d['instrument']: d
            for d in self.get_prices(
                account_id=self.__account_id,
                instruments=','.join(self.inst_dict.keys())
            )['prices'] if 'instrument' in d
        }

    def _refresh_pos_dict_and_pos_time(self):
        p0 = self.pos_dict
        self.pos_dict = {
            d['instrument']: d for d
            in self.get_positions(account_id=self.__account_id)['positions']
            if 'instrument' in d
        }
        for i in self.instruments:
            if not self.pos_dict.get(i):
                self.__pos_time[i] = None
            elif not p0.get(i) or p0[i]['side'] != self.pos_dict[i]['side']:
                self.__pos_time[i] = datetime.now()

    def _refresh_unit_costs(self):
        self.unit_costs = {
            i: self._calculate_bp_value(instrument=i) * d['marginRate']
            for i, d in self.inst_dict.items() if i in self.instruments
        }

    def _calculate_bp_value(self, instrument):
        cur_pair = instrument.split('_')
        if cur_pair[0] == self.acc_dict['accountCurrency']:
            bpv = 1 / self.__rate_dict[instrument]['ask']
        elif cur_pair[1] == self.acc_dict['accountCurrency']:
            bpv = self.__rate_dict[instrument]['ask']
        else:
            inst_bpv = [
                i for i in self.inst_dict.keys() if set(i.split('_')) == {
                    cur_pair[1], self.acc_dict['accountCurrency']
                }
            ][0]
            bpv = self.__rate_dict[instrument]['ask'] * (
                self.__rate_dict[inst_bpv]['ask']
                if inst_bpv.split('_')[1] == self.acc_dict['accountCurrency']
                else (1 / self.__rate_dict[inst_bpv]['ask'])
            )
        return bpv

    def design_and_place_order(self, instrument, act):
        pos = self.pos_dict.get(instrument)
        if act and pos and (act == 'close' or act != pos['side']):
            self.__logger.info('Close a position: {}'.format(pos['side']))
            self._place_order(closing=True, instrument=instrument)
            self._refresh_txn_list()
        if act in ['buy', 'sell']:
            limits = self._design_order_limits(instrument=instrument, side=act)
            self.__logger.debug('limits: {}'.format(limits))
            units = self._design_order_units(instrument=instrument, side=act)
            self.__logger.debug('units: {}'.format(units))
            self.__logger.info('Open a order: {}'.format(act))
            self._place_order(
                type='market', instrument=instrument, side=act, units=units,
                **limits
            )

    def _place_order(self, closing=False, **kwargs):
        try:
            if self.__dry_run:
                r = {
                    'func': 'close_position' if closing else 'create_order',
                    'args': {'account_id': self.__account_id, **kwargs}
                }
            elif closing:
                r = self.close_position(account_id=self.__account_id, **kwargs)
            else:
                r = self.create_order(account_id=self.__account_id, **kwargs)
        except Exception as e:
            self.__logger.error(e)
            if self.__order_log_path:
                self._write_data(e, path=self.__order_log_path)
        else:
            self.__logger.info(os.linesep + pformat(r))
            if self.__order_log_path:
                self._write_data(json.dumps(r), path=self.__order_log_path)
            else:
                time.sleep(0.5)

    def _design_order_limits(self, instrument, side):
        rate = self.__rate_dict[instrument][
            {'buy': 'ask', 'sell': 'bid'}[side]
        ]
        ts_in_cf = int(
            self.cf['position']['limit_price_ratio']['trailing_stop'] *
            rate / np.float32(self.inst_dict[instrument]['pip'])
        )
        trailing_stop = min(
            max(ts_in_cf, self.inst_dict[instrument]['minTrailingStop']),
            self.inst_dict[instrument]['maxTrailingStop']
        )
        tp = {
            k: np.float16(
                rate + rate * v * {
                    'take_profit': {'buy': 1, 'sell': -1}[side],
                    'stop_loss': {'buy': -1, 'sell': 1}[side],
                    'upper_bound': 1, 'lower_bound': -1
                }[k]
            ) for k, v in self.cf['position']['limit_price_ratio'].items()
            if k in ['take_profit', 'stop_loss', 'upper_bound', 'lower_bound']
        }
        return {
            'trailingStop': trailing_stop, 'takeProfit': tp['take_profit'],
            'stopLoss': tp['stop_loss'], 'upperBound': tp['upper_bound'],
            'lowerBound': tp['lower_bound']
        }

    def _design_order_units(self, instrument, side):
        avail_size = max(
            np.ceil(
                (
                    self.acc_dict['marginAvail'] - self.acc_dict['balance'] *
                    self.cf['position']['margin_nav_ratio']['preserve']
                ) / self.unit_costs[instrument]
            ), 0
        )
        self.__logger.debug('avail_size: {}'.format(avail_size))
        sizes = {
            k: np.ceil(
                self.acc_dict['balance'] * v / self.unit_costs[instrument]
            ) for k, v in self.cf['position']['margin_nav_ratio'].items()
            if k in ['unit', 'init']
        }
        self.__logger.debug('sizes: {}'.format(sizes))
        bet_size = self.__bs.calculate_size_by_pl(
            unit_size=sizes['unit'], init_size=sizes['init'],
            inst_txns=[
                t for t in self.__txn_list if t.get('instrument') == instrument
            ]
        )
        self.__logger.debug('bet_size: {}'.format(bet_size))
        return int(min(bet_size, avail_size))

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
            t['pl'] for t in self.__txn_list
            if t.get('pl') and t.get('instrument') == i
        ])
        self.print_log(
            '|{0:^33}|{1:^13}|'.format(
                '{0:>7}:{1:>21}'.format(
                    i.replace('_', '/'),
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
        return pd.DataFrame(
            self.get_history(
                account_id=self.__account_id, candleFormat='bidask',
                instrument=instrument, granularity=granularity,
                count=min(5000, int(count))
            )['candles']
        ).assign(
            time=lambda d: pd.to_datetime(d['time']), instrument=instrument
        ).set_index('time', drop=True)

    def fetch_latest_rate_df(self, instrument):
        return pd.DataFrame(
            self.get_prices(
                account_id=self.__account_id, instruments=instrument
            )['prices']
        ).assign(
            time=lambda d: pd.to_datetime(d['time'])
        ).set_index('time')


class BaseTrader(TraderCoreAPI, metaclass=ABCMeta):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.__ai = self.create_ai(model=model)

    def _create_ai(self, model, **kwargs):
        if model == 'ewma':
            return Ewma(config_dict=self.cf, **kwargs)
        else:
            raise FractRuntimeError('invalid model name: {}'.format(model))

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self.check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            for i in self.instruments:
                self.refresh_oanda_dicts()
                self.make_decision(instrument=i)

    @abstractmethod
    def check_health(self):
        return True

    @abstractmethod
    def make_decision(self, instrument):
        pass

    def determine_sig_state(self, df_rate):
        i = df_rate['instrument'].iloc[-1]
        pos = self.pos_dict.get(i)
        pos_pct = int(
            (
                pos['units'] * self.unit_costs[i] * 100 /
                self.acc_dict['balance']
            ) if pos else 0
        )
        sig = self.__ai.detect_signal(
            history_dict=self.fetch_history_dict(instrument=i), pos=pos
        )
        if self.inst_dict[i]['halted']:
            act = None
            state = 'TRADING HALTED'
        elif sig['sig_act'] == 'close':
            act = 'close'
            state = 'CLOSING'
        elif self.acc_dict['balance'] == 0:
            act = None
            state = 'NO FUND'
        elif self._is_margin_lack(instrument=i):
            act = None
            state = 'LACK OF FUNDS'
        elif self._is_over_spread(df_rate=df_rate):
            act = None
            state = 'OVER-SPREAD'
        elif sig['sig_act'] == 'buy':
            if pos and pos['side'] == 'buy':
                act = None
                state = '{:.1f}% LONG'.format(pos_pct)
            elif pos and pos['side'] == 'sell':
                act = 'buy'
                state = 'SHORT -> LONG'
            else:
                act = 'buy'
                state = '-> LONG'
        elif sig['sig_act'] == 'sell':
            if pos and pos['side'] == 'sell':
                act = None
                state = '{:.1f}% SHORT'.format(pos_pct)
            elif pos and pos['side'] == 'buy':
                act = 'sell'
                state = 'LONG -> SHORT'
            else:
                act = 'sell'
                state = '-> SHORT'
        elif pos and pos['side'] == 'buy':
            act = None
            state = '{:.1f}% LONG'.format(pos_pct)
        elif pos and pos['side'] == 'sell':
            act = None
            state = '{:.1f}% SHORT'.format(pos_pct)
        else:
            act = None
            state = '-'
        log_str = sig['sig_log_str'] + '{:^18}|'.format(state)
        return {'act': act, 'state': state, 'log_str': log_str, **sig}

    @abstractmethod
    def fetch_history_dict(self, instrument):
        pass

    def _is_margin_lack(self, instrument):
        return (
            not self.pos_dict.get(instrument) and
            self.acc_dict['marginAvail'] <= self.acc_dict['balance'] *
            self.cf['position']['margin_nav_ratio']['preserve']
        )

    def _is_over_spread(self, df_rate):
        return (
            df_rate.tail(n=1).pipe(
                lambda d: (d['ask'] - d['bid']) / (d['ask'] + d['bid']) * 2
            ).values[0] >=
            self.cf['position']['limit_price_ratio']['max_spread']
        )
