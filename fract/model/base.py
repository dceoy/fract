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
        self.logger = logging.getLogger(__name__)
        self.cf = config_dict
        super().__init__(
            environment=self.cf['oanda']['environment'],
            access_token=self.cf['oanda']['access_token']
        )
        self.account_id = self.cf['oanda']['account_id']
        self.instruments = (instruments or self.cf['instruments'])
        self.bs = BettingSystem(strategy=self.cf['position']['bet'])
        self.quiet = quiet
        self.dry_run = dry_run
        self.min_txn_id = max(
            [
                t['id'] for t in self.get_transaction_history(
                    account_id=self.account_id
                )['transactions']
            ] + [0]
        ) + 1
        if log_dir_path:
            self.log_dir_path = os.path.abspath(
                os.path.expanduser(os.path.expandvars(log_dir_path))
            )
            os.makedirs(self.log_dir_path, exist_ok=True)
            self.order_log_path = os.path.join(
                self.log_dir_path, 'order.json.txt'
            )
            self.txn_log_path = os.path.join(
                self.log_dir_path, 'txn.json.txt'
            )
            self._write_data(
                yaml.dump(
                    {
                        'instrument': self.instruments,
                        'model': self.cf['model'],
                        'position': self.cf['position']
                    },
                    default_flow_style=False
                ).strip(),
                path=os.path.join(self.log_dir_path, 'parameter.yml'),
                mode='w', append_linesep=False
            )
        else:
            self.log_dir_path = None
            self.order_log_path = None
            self.txn_log_path = None
        self.acc_dict = dict()
        self.txn_list = list()
        self.inst_dict = dict()
        self.rate_dict = dict()
        self.unit_costs = dict()
        self.pos_dict = dict()
        self.pos_time = dict()

    def expire_positions(self, ttl_sec=86400):
        for i, p in self.pos_dict.items():
            if self.pos_time.get(i):
                et_sec = (datetime.now() - self.pos_time[i]).total_seconds()
                self.logger.info('{0} => {1} sec elapsed'.format(p, et_sec))
                if et_sec > ttl_sec:
                    self.logger.info('Close a position: {}'.format(p['side']))
                    self._place_order(closing=True, instrument=i)

    def refresh_oanda_dicts(self):
        t0 = datetime.now()
        self.acc_dict = self.get_account(account_id=self.account_id)
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
                account_id=self.account_id, minId=self.min_txn_id
            ).get('transactions')
            if t['id'] not in [t['id'] for t in self.txn_list]
        ]
        if th_new:
            self.print_log(yaml.dump(th_new, default_flow_style=False).strip())
            self.txn_list = self.txn_list + th_new
            if self.txn_log_path:
                self._write_data(json.dumps(th_new), path=self.txn_log_path)

    def _refresh_inst_dict(self):
        self.inst_dict = {
            d['instrument']: d
            for d in self.get_instruments(
                account_id=self.account_id,
                fields=','.join([
                    'displayName', 'pip', 'maxTradeUnits', 'precision',
                    'maxTrailingStop', 'minTrailingStop', 'marginRate',
                    'halted'
                ])
            )['instruments'] if 'instrument' in d
        }

    def _refresh_rate_dict(self):
        self.rate_dict = {
            d['instrument']: d
            for d in self.get_prices(
                account_id=self.account_id,
                instruments=','.join(self.inst_dict.keys())
            )['prices'] if 'instrument' in d
        }

    def _refresh_pos_dict_and_pos_time(self):
        p0 = self.pos_dict
        self.pos_dict = {
            d['instrument']: d for d
            in self.get_positions(account_id=self.account_id)['positions']
            if 'instrument' in d
        }
        for i in self.instruments:
            if not self.pos_dict.get(i):
                self.pos_time[i] = None
            elif not p0.get(i) or p0[i]['side'] != self.pos_dict[i]['side']:
                self.pos_time[i] = datetime.now()

    def _refresh_unit_costs(self):
        self.unit_costs = {
            i: self._calculate_bp_value(instrument=i) * d['marginRate']
            for i, d in self.inst_dict.items() if i in self.instruments
        }

    def _calculate_bp_value(self, instrument):
        cur_pair = instrument.split('_')
        if cur_pair[0] == self.acc_dict['accountCurrency']:
            bpv = 1 / self.rate_dict[instrument]['ask']
        elif cur_pair[1] == self.acc_dict['accountCurrency']:
            bpv = self.rate_dict[instrument]['ask']
        else:
            inst_bpv = [
                i for i in self.inst_dict.keys()
                if set(i.split('_')) == {
                    cur_pair[1], self.acc_dict['accountCurrency']
                }
            ][0]
            bpv = self.rate_dict[instrument]['ask'] * (
                self.rate_dict[inst_bpv]['ask']
                if inst_bpv.split('_')[1] == self.acc_dict['accountCurrency']
                else (1 / self.rate_dict[inst_bpv]['ask'])
            )
        return bpv

    def design_and_place_order(self, instrument, act):
        pos = self.pos_dict.get(instrument)
        if act and pos and (act == 'close' or act != pos['side']):
            self.logger.info('Close a position: {}'.format(pos['side']))
            self._place_order(closing=True, instrument=instrument)
            self._refresh_txn_list()
        if act in ['buy', 'sell']:
            limits = self._design_order_limits(instrument=instrument, side=act)
            self.logger.debug('limits: {}'.format(limits))
            units = self._design_order_units(instrument=instrument, side=act)
            self.logger.debug('units: {}'.format(units))
            self.logger.info('Open a order: {}'.format(act))
            self._place_order(
                type='market', instrument=instrument, side=act, units=units,
                **limits
            )

    def _place_order(self, closing=False, **kwargs):
        try:
            if self.dry_run:
                r = {
                    'func': 'close_position' if closing else 'create_order',
                    'args': {'account_id': self.account_id, **kwargs}
                }
            elif closing:
                r = self.close_position(account_id=self.account_id, **kwargs)
            else:
                r = self.create_order(account_id=self.account_id, **kwargs)
        except Exception as e:
            self.logger.error(e)
            if self.order_log_path:
                self._write_data(e, path=self.order_log_path)
        else:
            self.logger.info(os.linesep + pformat(r))
            if self.order_log_path:
                self._write_data(json.dumps(r), path=self.order_log_path)
            else:
                time.sleep(0.5)

    def _design_order_limits(self, instrument, side):
        rate = self.rate_dict[instrument][{'buy': 'ask', 'sell': 'bid'}[side]]
        trailing_stop = min(
            max(
                int(
                    self.cf['position']['limit_price_ratio']['trailing_stop'] *
                    rate / np.float32(self.inst_dict[instrument]['pip'])
                ),
                self.inst_dict[instrument]['minTrailingStop']
            ),
            self.inst_dict[instrument]['maxTrailingStop']
        )
        tp = {
            k: np.float16(
                rate + rate * v * {
                    'take_profit': {'buy': 1, 'sell': -1}[side],
                    'stop_loss': {'buy': -1, 'sell': 1}[side],
                    'upper_bound': 1, 'lower_bound': -1
                }[k]
            )
            for k, v in self.cf['position']['limit_price_ratio'].items()
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
        self.logger.debug('avail_size: {}'.format(avail_size))
        sizes = {
            k: np.ceil(
                self.acc_dict['balance'] * v / self.unit_costs[instrument]
            ) for k, v in self.cf['position']['margin_nav_ratio'].items()
            if k in ['unit', 'init']
        }
        self.logger.debug('sizes: {}'.format(sizes))
        bet_size = self.bs.calculate_size_by_pl(
            unit_size=sizes['unit'], init_size=sizes['init'],
            inst_txns=[
                t for t in self.txn_list if t.get('instrument') == instrument
            ]
        )
        self.logger.debug('bet_size: {}'.format(bet_size))
        return int(min(bet_size, avail_size))

    @staticmethod
    def _sleep(last, sec=0.5):
        rest = sec - (datetime.now() - last).total_seconds()
        if rest > 0:
            time.sleep(rest)

    def print_log(self, data):
        if self.quiet:
            self.logger.info(data)
        else:
            print(data, flush=True)

    def print_state_line(self, df_rate, add_str):
        i = df_rate['instrument'].iloc[-1]
        net_pl = sum([
            t['pl'] for t in self.txn_list
            if t.get('pl') and t.get('instrument') == i
        ])
        self.print_log(
            '|{0:^33}|{1:^15}|'.format(
                '{0:>7}:{1:>21}'.format(
                    i.replace('_', '/'),
                    np.array2string(
                        df_rate[['bid', 'ask']].iloc[-1].values,
                        formatter={'float_kind': lambda f: '{:8g}'.format(f)}
                    )
                ),
                'PL:{:>8}'.format(int(net_pl))
            ) + (add_str or '')
        )

    def _write_data(self, data, path, mode='a', append_linesep=True):
        with open(path, mode) as f:
            f.write(str(data) + (os.linesep if append_linesep else ''))

    def write_log_df(self, name, df):
        if self.log_dir_path and df.size:
            self.logger.debug('{0} df:{1}{2}'.format(name, os.linesep, df))
            p = os.path.join(self.log_dir_path, '{}.tsv'.format(name))
            self.logger.info('Write TSV log: {}'.format(p))
            self._write_df(df=df, path=p)

    def _write_df(self, df, path, mode='a'):
        df.to_csv(
            path, mode=mode, sep=(',' if path.endswith('.csv') else '\t'),
            header=(not os.path.isfile(path))
        )

    def fetch_candle(self, instrument, granularity='S5', count=5000):
        return self.get_history(
            account_id=self.account_id, candleFormat='bidask',
            instrument=instrument, granularity=granularity,
            count=min(5000, int(count))
        )['candles']


class BaseTrader(TraderCoreAPI, metaclass=ABCMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def invoke(self):
        self.print_log('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while self.check_health():
            self.expire_positions(ttl_sec=self.cf['position']['ttl_sec'])
            for i in self.instruments:
                self.refresh_oanda_dicts()
                df_r = self.fetch_rate_df(instrument=i)
                if df_r.size:
                    self.update_caches(df_rate=df_r)
                    st = self.determine_sig_state(df_rate=df_r)
                    self.print_state_line(df_rate=df_r, add_str=st['log_str'])
                    self.design_and_place_order(instrument=i, act=st['act'])
                    self.write_log_df(
                        name='rate.{}'.format(i),
                        df=df_r.drop(columns=['instrument'])
                    )
                    self.write_log_df(
                        name='sig.{}'.format(i),
                        df=pd.DataFrame([st]).drop(
                            columns=['log_str', 'sig_log_str']
                        ).assign(
                            time=df_r.index[-1], ask=df_r['ask'].iloc[-1],
                            bid=df_r['bid'].iloc[-1]
                        ).set_index('time', drop=True)
                    )
                else:
                    self.logger.debug('no updated rate')

    @abstractmethod
    def check_health(self):
        return True

    @abstractmethod
    def fetch_rate_df(self, instrument):
        return pd.DataFrame()

    @abstractmethod
    def update_caches(self, df_rate):
        pass

    @abstractmethod
    def determine_sig_state(self, df_rate):
        return {'act': None, 'log_str': ''}

    def create_ai(self, model):
        if model == 'ewma':
            return Ewma(config_dict=self.cf)
        else:
            raise FractRuntimeError('invalid model name: {}'.format(model))

    def is_margin_lack(self, instrument):
        return (
            not self.pos_dict.get(instrument) and
            self.acc_dict['marginAvail'] <= self.acc_dict['balance'] *
            self.cf['position']['margin_nav_ratio']['preserve']
        )

    def is_over_spread(self, df_rate):
        return (
            df_rate.tail(n=1).pipe(
                lambda d: (d['ask'] - d['bid']) / (d['ask'] + d['bid']) * 2
            ).values[0] >=
            self.cf['position']['limit_price_ratio']['max_spread']
        )
