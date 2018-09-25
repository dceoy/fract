#!/usr/bin/env python

import logging
from multiprocessing import cpu_count
import signal
import time
from ..cli.util import FractError, read_config_yml
from ..model.bollinger import Bollinger
from ..model.delta import Delta
from ..model.ewma import EwmLogDiffTrader
from ..model.kalman import Kalman
from ..model.volatility import Volatility


def invoke_trader(config_yml, instruments=None, model='ewma', interval='0',
                  timeout='3600', with_streamer=False, redis_host=None,
                  redis_port='6379', redis_db='0', quiet=False):
    logger = logging.getLogger(__name__)
    logger.info('Autonomous trading')
    cf = read_config_yml(path=config_yml)
    if model == 'ewma':
        trader = EwmLogDiffTrader(
            config_dict=cf, instruments=instruments, redis_host=redis_host,
            redis_port=int(redis_port), redis_db=int(redis_db),
            interval=int(interval), timeout=int(timeout), n_cpu=cpu_count(),
            with_streamer=with_streamer, quiet=quiet
        )
        logger.info('Invoke a trader')
        trader.invoke()
    else:
        if model == 'volatility':
            trader = Volatility(
                oanda=cf['oanda'], model=cf['trade']['model']['volatility'],
                margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
            )
        elif model == 'delta':
            trader = Delta(
                oanda=cf['oanda'], model=cf['trade']['model']['delta'],
                margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
            )
        elif model == 'bollinger':
            trader = Bollinger(
                oanda=cf['oanda'], model=cf['trade']['model']['bollinger'],
                margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
            )
        elif model == 'kalman':
            trader = Kalman(
                oanda=cf['oanda'], model=cf['trade']['model']['kalman'],
                margin_ratio=cf['trade']['margin_ratio'], quiet=quiet
            )
        else:
            raise FractError('invalid trading model')
        insts = (instruments if instruments else cf['instruments'])
        if not quiet:
            print('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while True:
            if all([trader.fire(instrument=i)['halted'] for i in insts]):
                break
            else:
                time.sleep(int(interval))
