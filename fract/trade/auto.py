#!/usr/bin/env python

import signal
import time
from ..cli.util import FractError
from ..model.bollinger import Bollinger
from ..model.kalman import Kalman


def open_deals(config, instruments, model, n=10, interval=2, quiet=False):
    insts = (instruments if instruments else config['trade']['instruments'])
    if model not in ['bollinger', 'kalman']:
        raise FractError('invalid trading model')
    elif model not in config['trade']['model']:
        raise FractError('`{}` not in config'.format(model))
    elif not quiet:
        print('!!! OPEN DEALS !!!')

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if model == 'kalman':
        trader = Kalman(oanda=config['oanda'],
                        model=config['trade']['model']['kalman'],
                        margin_ratio=config['trade']['margin_ratio'],
                        quiet=quiet)
    elif model == 'bollinger':
        trader = Bollinger(oanda=config['oanda'],
                           model=config['trade']['model']['bollinger'],
                           margin_ratio=config['trade']['margin_ratio'],
                           quiet=quiet)

    for i in range(n):
        halted = all([
            trader.fire(instrument=inst)['halted'] for inst in insts
        ])
        if halted or i == n - 1:
            break
        else:
            time.sleep(interval)
