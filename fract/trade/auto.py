#!/usr/bin/env python

import signal
import time
from ..cli.util import FractError
from ..model.bollinger import Bollinger
from ..model.kalman import Kalman
from ..model.delta import Delta


def open_deals(config, instruments, model, n=10, interval=2, quiet=False):
    insts = (instruments if instruments else config['trade']['instruments'])
    if model not in ['bollinger', 'delta', 'delta_bollinger', 'kalman']:
        raise FractError('invalid trading model')
    else:
        for m in model.split('_'):
            if m not in config['trade']['model']:
                raise FractError('`{}` not in config'.format(m))

    if not quiet:
        print('!!! OPEN DEALS !!!')
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    if model == 'delta':
        traders = [
            Delta(oanda=config['oanda'],
                  model=config['trade']['model']['delta'],
                  margin_ratio=config['trade']['margin_ratio'],
                  quiet=quiet)
        ]
    elif model == 'bollinger':
        traders = [
            Bollinger(oanda=config['oanda'],
                      model=config['trade']['model']['bollinger'],
                      margin_ratio=config['trade']['margin_ratio'],
                      quiet=quiet)
        ]
    elif model == 'delta_bollinger':
        traders = [
            Delta(oanda=config['oanda'],
                  model=config['trade']['model']['delta'],
                  margin_ratio=config['trade']['margin_ratio'],
                  quiet=quiet),
            Bollinger(oanda=config['oanda'],
                      model=config['trade']['model']['bollinger'],
                      margin_ratio=config['trade']['margin_ratio'],
                      quiet=quiet)
        ]
    elif model == 'kalman':
        traders = [
            Kalman(oanda=config['oanda'],
                   model=config['trade']['model']['kalman'],
                   margin_ratio=config['trade']['margin_ratio'],
                   quiet=quiet)
        ]

    for i in range(n):
        halted = all([
            all([
                t.fire(instrument=inst)['halted']
                for t in traders
            ])
            for inst in insts
        ])
        if halted or i == n - 1:
            break
        else:
            time.sleep(interval)
