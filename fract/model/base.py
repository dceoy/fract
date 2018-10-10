#!/usr/bin/env python

from datetime import datetime
import json
import logging
import os
from pprint import pformat
import time
import numpy as np
import oandapy
import yaml
from .bet import BettingSystem


class BaseTrader(oandapy.API):
    def __init__(self, config_dict, instruments, log_dir_path=None,
                 quiet=False):
        self.logger = logging.getLogger(__name__)
        self.cf = config_dict
        super().__init__(
            environment=config_dict['oanda']['environment'],
            access_token=config_dict['oanda']['access_token']
        )
        self.account_id = config_dict['oanda']['account_id']
        self.instruments = instruments or config_dict['instruments']
        self.quiet = quiet
        self.bs = BettingSystem(strategy=self.cf['position']['bet'])
        self.min_txn_id = max(
            [
                t['id'] for t in self.get_transaction_history(
                    account_id=self.account_id
                )['transactions']
            ] + [0]
        ) + 1
        self.acc_dict = dict()
        self.txn_list = list()
        self.inst_dict = dict()
        self.rate_dict = dict()
        self.unit_costs = dict()
        self.pos_dict = dict()
        self.pos_time = dict()
        if log_dir_path:
            self.log_dir_path = self._abspath(log_dir_path)
            self.order_log_path = os.path.join(
                self.log_dir_path, 'order.json.txt'
            )
            self.txn_log_path = os.path.join(
                self.log_dir_path, 'txn.json.txt'
            )
            self.write_data(
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

    @staticmethod
    def _abspath(path):
        return os.path.abspath(os.path.expanduser(path))

    def refresh_oanda_dicts(self):
        t0 = datetime.now()
        self.acc_dict = self.get_account(account_id=self.account_id)
        self.sleep(last=t0, sec=0.5)
        self._refresh_txn_list()
        self.sleep(last=t0, sec=1)
        self._refresh_inst_dict()
        self.sleep(last=t0, sec=1.5)
        self._refresh_rate_dict()
        self._refresh_unit_costs()
        self.sleep(last=t0, sec=2)
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
                self.write_data(json.dumps(th_new), path=self.txn_log_path)

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

    def expire_positions(self, ttl_sec=86400):
        inst_times = {
            p['instrument']: self.pos_time[p['instrument']] for p in
            self.get_positions(account_id=self.account_id)['positions']
            if self.pos_time.get(p['instrument'])
        }
        for i, t in inst_times.items():
            if (datetime.now() - t).total_seconds() > ttl_sec:
                self._place_order(closing=True, instrument=i)

    def design_and_place_order(self, instrument, side):
        pos = self.pos_dict.get(instrument)
        limits = self.design_order_limits(instrument=instrument, side=side)
        self.logger.debug('limits: {}'.format(limits))
        if pos and pos['side'] != side:
            self.logger.info('Close a position: {}'.format(pos['side']))
            self._place_order(closing=True, instrument=instrument)
            self._refresh_txn_list()
        units = self.design_order_units(instrument=instrument, side=side)
        self.logger.debug('units: {}'.format(units))
        self.logger.info('Open a order: {}'.format(side))
        self._place_order(
            type='market', instrument=instrument, side=side, units=units,
            **limits
        )

    def _place_order(self, closing=False, **kwargs):
        try:
            if closing:
                r = self.close_position(account_id=self.account_id, **kwargs)
            else:
                r = self.create_order(account_id=self.account_id, **kwargs)
        except Exception as e:
            self.logger.error(e)
            if self.order_log_path:
                self.write_data(e, path=self.order_log_path)
        else:
            self.logger.info(os.linesep + pformat(r))
            if self.order_log_path:
                self.write_data(json.dumps(r), path=self.order_log_path)
            else:
                time.sleep(0.5)

    def design_order_limits(self, instrument, side):
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

    def design_order_units(self, instrument, side):
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
    def sleep(last, sec=0.5):
        rest = sec - (datetime.now() - last).total_seconds()
        if rest > 0:
            time.sleep(rest)

    def print_log(self, data):
        if self.quiet:
            self.logger.info(data)
        else:
            print(data, flush=True)

    def write_data(self, data, path, mode='a', append_linesep=True):
        with open(self._abspath(path), mode) as f:
            f.write(str(data) + (os.linesep if append_linesep else ''))

    def write_log_df(self, name, df):
        if self.log_dir_path and df.size:
            self.logger.debug('{0} df:{1}{2}'.format(name, os.linesep, df))
            p = os.path.join(self.log_dir_path, '{}.tsv'.format(name))
            self.logger.info('Write TSV log: {}'.format(p))
            self._write_df(df=df, path=p)

    def _write_df(self, df, path, mode='a'):
        p = self._abspath(path)
        df.to_csv(
            p, mode=mode, sep=(',' if p.endswith('.csv') else '\t'),
            header=(not os.path.isfile(p))
        )
