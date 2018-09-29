#!/usr/bin/env python

import json
import logging
import os
from pprint import pformat
import time
import numpy as np
import oandapy
import pandas as pd
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
        self.account_id = config_dict['oanda']['account_id'],
        self.instruments = instruments or config_dict['instruments']
        if log_dir_path:
            log_dir_path = os.path.abspath(os.path.expanduser(log_dir_path))
            self.log_dir_path = log_dir_path
            self.order_log_path = os.path.join(log_dir_path, 'order.json.txt')
        else:
            self.log_dir_path = None
            self.order_log_path = None
        self.quiet = quiet
        self.bs = BettingSystem(strategy=self.cf['position']['bet'])
        self.oanda_dict = dict()
        self.history_books = {i: pd.DataFrame() for i in self.instruments}
        self.logger.debug('vars(self): ' + pformat(vars(self)))

    def close_positions(self, instruments=[]):
        return [
            self._place_close_order(instrument=i) for i in {
                p['instrument'] for p in
                self.get_positions(account_id=self.account_id)['positions']
            } if not instruments or i in instruments
        ]

    def _place_close_order(self, instrument):
        try:
            r = self.close_position(
                account_id=self.account_id, instrument=instrument
            )
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info('Close an position:' + os.linesep + pformat(r))
            return r
        finally:
            time.sleep(0.5)

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
        for i in self.instruments:
            df_p = pd.DataFrame(positions['positions']).pipe(
                lambda d: d[d['instrument'] == i]
            )
            df_r = pd.DataFrame(prices['prices']).pipe(
                lambda d: d[d['instrument'] == i][['time', 'bid', 'ask']]
            )
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
        insts = {
            p['instrument'] for p in
            self.get_positions(account_id=self.account_id)['positions']
            if self.history_books[p['instrument']].size and
            self.history_books[p['instrument']]['pl'].values[-1] is np.nan
        }
        if insts:
            time.sleep(0.5)
            latest_datetimes = {
                r['instrument']: pd.to_datetime(r['time'])
                for r in self.get_prices(
                    account_id=self.account_id, instruments=','.join(insts)
                )['prices']
            }
            for i, t in latest_datetimes.items():
                td = (t - self.history_books[i]['time'].tail(n=1)).value[0]
                if td > np.timedelta64(ttl_sec, 's'):
                    r = self._place_close_order(instrument=i)
                    if self.log_dir_path:
                        self.write_json_log(data=r, name=self.order_log_path)

    def design_and_place_order(self, instrument, side):
        units = self.calculate_order_units(instrument=instrument, side=side)
        limits = self.calculate_order_limits(instrument=instrument, side=side)
        try:
            r = self.create_order(
                account_id=self.account_id, type='market',
                instrument=instrument, side=side, units=units, **limits
            )
        except Exception as e:
            self.logger.error(e)
        else:
            self.print_log('Open on order:' + os.linesep + pformat(r))
        finally:
            if self.log_dir_path:
                self.write_json_log(data=r, name=self.order_log_path)

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

    def calculate_order_units(self, instrument, side):
        margin_per_bp = self.calculate_bp_value(instrument=instrument) * [
            i for i in self.oanda_dict['instruments'] if i == instrument
        ][0]['marginRate']
        avail_size = np.ceil(
            (
                self.oanda_dict['marginAvail'] - self.oanda_dict['balance'] *
                self.cf['position']['margin_nav_ratio']['preserve']
            ) / margin_per_bp
        )
        sizes = {
            k: np.ceil(self.oanda_dict['balance'] * v / margin_per_bp)
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

    def print_log(self, data):
        if self.quiet:
            self.logger.info(data)
        else:
            print(data, flush=True)

    def write_df_log(self, df, name, mode='a'):
        p = os.path.join(self.log_dir_path, name)
        df.to_csv(p, mode=mode, header=(not os.path.isfile(p)))

    def write_json_log(self, data, name, mode='a'):
        with open(os.path.join(self.log_dir_path, name), mode) as f:
            f.write(json.dumps(data) + os.linesep)

    def write_parameter_log(self, name='fract.parameter.yml'):
        param = {
            'instrument': self.instruments, 'model': self.cf['model'],
            'position': self.cf['position']
        }
        with open(os.path.join(self.log_dir_path, name), 'w') as f:
            f.write(yaml.dump(param, default_flow_style=False))
