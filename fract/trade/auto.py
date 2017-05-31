#!/usr/bin/env python

import signal
import time
from ..model.bollinger import Bollinger
from ..model.kalman import Kalman


def open_deals(config, instruments, n=10, interval=2, quiet=False):
    insts = (instruments if instruments else config['trade']['instruments'])
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('!!! OPEN DEALS !!!')
    k = Kalman(oanda=config['oanda'],
               model=config['trade']['model']['kalman'],
               margin_ratio=config['trade']['margin_ratio'],
               quiet=quiet)
    b = Bollinger(oanda=config['oanda'],
                  model=config['trade']['model']['bollinger'],
                  margin_ratio=config['trade']['margin_ratio'],
                  quiet=quiet)
    for i in range(n):
        halted = all([
            any([k.fire(instrument=inst)['halted'],
                 b.fire(instrument=inst)['halted']])
            for inst in insts
        ])
        if halted or i == n - 1:
            break
        else:
            time.sleep(interval)
