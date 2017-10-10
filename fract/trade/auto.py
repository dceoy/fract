#!/usr/bin/env python

import signal
import time
from ..cli.util import FractError
from ..model.bollinger import Bollinger
from ..model.kalman import Kalman
from ..model.delta import Delta
from ..model.volatility import Volatility


def open_deals(config, instruments, models, n=10, interval=2, quiet=False):
    insts = (instruments if instruments else config['trade']['instruments'])
    traders = [
        _generate_trader(model=m, config=config, quiet=quiet)
        for m in models.split(',')
    ]
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('!!! OPEN DEALS !!!')
    for i in range(n):
        res = traders[i % len(traders)].fire(
            instrument=insts[i % (len(traders) * len(insts)) // len(traders)]
        )
        if res['halted'] or i == n - 1:
            break
        else:
            time.sleep(interval)


def _generate_trader(model, config, quiet=False):
    if model not in config['trade']['model']:
        raise FractError('`{}` not in config'.format(model))
    elif model == 'volatility':
        return Volatility(oanda=config['oanda'],
                          model=config['trade']['model']['volatility'],
                          margin_ratio=config['trade']['margin_ratio'],
                          quiet=quiet)
    elif model == 'delta':
        return Delta(oanda=config['oanda'],
                     model=config['trade']['model']['delta'],
                     margin_ratio=config['trade']['margin_ratio'],
                     quiet=quiet)
    elif model == 'bollinger':
        return Bollinger(oanda=config['oanda'],
                         model=config['trade']['model']['bollinger'],
                         margin_ratio=config['trade']['margin_ratio'],
                         quiet=quiet)
    elif model == 'kalman':
        return Kalman(oanda=config['oanda'],
                      model=config['trade']['model']['kalman'],
                      margin_ratio=config['trade']['margin_ratio'],
                      quiet=quiet)
    else:
        raise FractError('invalid trading model')
