#!/usr/bin/env python

import logging
from multiprocessing import cpu_count
import signal
import time
from .streamer import StreamDriver
from ..cli.util import FractError, read_config_yml
from ..model.bollinger import Bollinger
from ..model.delta import Delta
from ..model.ewma import Ewma
from ..model.kalman import Kalman
from ..model.volatility import Volatility


def open_deals(config_yml, instruments, redis_host, redis_port=6379,
               redis_db=0, redis_max_llen=None, wait=0, timeout=3600,
               without_streamer=False, quiet=False, model='ewma'):
    logger = logging.getLogger(__name__)
    logger.info('Autonomous trading')
    cf = read_config_yml(path=config_yml)
    insts = (instruments if instruments else cf['instruments'])
    if model == 'ewma':
        trader = Ewma(
            environment=cf['oanda']['environment'],
            access_token=cf['oanda']['access_token'],
            account_id=cf['oanda']['account_id'], instruments=insts,
            redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
            wait=wait, timeout=timeout, n_cpu=cpu_count(), quiet=quiet
        )
        if without_streamer:
            logger.info('Invoke a trader withoput an internal streamer')
        else:
            trader.set_streamer(
                streamer=StreamDriver(
                    environment=cf['oanda']['environment'],
                    access_token=cf['oanda']['access_token'],
                    account_id=cf['oanda']['account_id'], target='rate',
                    instruments=insts, ignore_heartbeat=True, use_redis=True,
                    redis_host=redis_host, redis_port=redis_port,
                    redis_db=redis_db, redis_max_llen=redis_max_llen,
                    quiet=True
                )
            )
            logger.info('Invoke a trader')
        trader.run()
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
        if not quiet:
            print('!!! OPEN DEALS !!!')
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        while True:
            if all([trader.fire(instrument=i)['halted'] for i in insts]):
                break
            else:
                time.sleep(wait)
