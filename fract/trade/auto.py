#!/usr/bin/env python

import logging
import signal
import time
from ..cli.util import FractError, read_config_yml
from ..model.bollinger import Bollinger
from ..model.kalman import Kalman
from ..model.delta import Delta
from ..model.volatility import Volatility


def open_deals(config_yml, instruments, models, n=10, interval=2, quiet=False):
    logger = logging.getLogger(__name__)
    logger.info('Autonomous trading')
    cf = read_config_yml(path=config_yml)
    insts = (instruments if instruments else cf['trade']['instruments'])
    traders = [_trader(model=m, cf=cf, quiet=quiet) for m in models.split(',')]
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


def _trader(model, cf, quiet=False):
    if model not in cf['trade']['model']:
        raise FractError('`{}` not in cf'.format(model))
    elif model == 'volatility':
        return Volatility(
            oanda=cf['oanda'], model=cf['trade']['model']['volatility'],
            margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
        )
    elif model == 'delta':
        return Delta(
            oanda=cf['oanda'], model=cf['trade']['model']['delta'],
            margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
        )
    elif model == 'bollinger':
        return Bollinger(
            oanda=cf['oanda'], model=cf['trade']['model']['bollinger'],
            margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
        )
    elif model == 'kalman':
        return Kalman(
            oanda=cf['oanda'], model=cf['trade']['model']['kalman'],
            margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
        )
    else:
        raise FractError('invalid trading model')
