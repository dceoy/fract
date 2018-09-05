#!/usr/bin/env python

from concurrent.futures import as_completed, ProcessPoolExecutor
import logging
import signal
from ..cli.util import read_config_yml
from ..model.online import FractRedisTrader
from .streamer import StreamDriver


def open_deals(config_yml, instruments, redis_host, redis_port=6379,
               redis_db=0, redis_maxl=1000, wait=0, timeout=3600, quiet=False):
    logger = logging.getLogger(__name__)
    logger.info('Autonomous trading')
    cf = read_config_yml(path=config_yml)
    insts = (instruments if instruments else cf['instruments'])
    streamer = StreamDriver(
        environment=cf['oanda']['environment'],
        access_token=cf['oanda']['access_token'],
        account_id=cf['oanda']['account_id'], target='rate',
        instruments=insts, ignore_heartbeat=True, use_redis=True,
        redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
        redis_maxl=redis_maxl, quiet=True
    )
    trader = FractRedisTrader(
        environment=cf['oanda']['environment'],
        access_token=cf['oanda']['access_token'],
        account_id=cf['oanda']['account_id'], instruments=insts,
        redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
        redis_maxl=redis_maxl, wait=wait, timeout=timeout, quiet=quiet
    )
    if not quiet:
        print('!!! OPEN DEALS !!!')
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    ppe = ProcessPoolExecutor(max_workers=2)
    fs = [ppe.submit(i.invoke) for i in [streamer, trader]]
    try:
        fs_results = [f.result() for f in as_completed(fs)]
    except Exception as e:
        [f.shutdown(wait=False) for f in fs]
        raise e
    else:
        logging.debug('fs_results: {}'.format(fs_results))
