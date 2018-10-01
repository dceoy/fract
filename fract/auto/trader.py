#!/usr/bin/env python

from concurrent.futures import as_completed, ProcessPoolExecutor, \
    ThreadPoolExecutor
import logging
from multiprocessing import cpu_count
import redis
from ..util.config import read_config_yml
from ..util.error import FractRuntimeError
from ..model.ewm import EwmLogDiffTrader
from .streamer import StreamDriver


def invoke_trader(config_yml, instruments=None, model='ewm', interval_sec=0,
                  timeout_sec=3600, with_streamer=False, redis_host=None,
                  redis_port=6379, redis_db=0, log_dir_path=None,
                  quiet=False):
    logger = logging.getLogger(__name__)
    logger.info('Autonomous trading')
    cf = read_config_yml(path=config_yml)
    rd = cf['redis'] if 'redis' in cf else {}
    if model == 'ewm':
        redis_pool = redis.ConnectionPool(
            host=(redis_host or rd.get('host')),
            port=(redis_port or rd.get('port')),
            db=(redis_db if redis_db is not None else rd.get('db'))
        )
        trader = EwmLogDiffTrader(
            config_dict=cf, instruments=instruments, redis_pool=redis_pool,
            interval_sec=interval_sec, timeout_sec=timeout_sec,
            log_dir_path=log_dir_path, quiet=quiet
        )
        if with_streamer:
            logger.info('Invoke a trader with a streamer')
            streamer = StreamDriver(
                config_dict=cf, target='rate', instruments=instruments,
                redis_pool=redis_pool, quiet=True
            )
            if cpu_count() > 1:
                executor = ProcessPoolExecutor(max_workers=2)
            else:
                executor = ThreadPoolExecutor(max_workers=2)
            fs = [
                executor.submit(lambda x: x.invoke(), i)
                for i in [streamer, trader]
            ]
            try:
                results = [f.result() for f in as_completed(fs)]
            except Exception as e:
                streamer.shutdown()
                trader.shutdown()
                executor.shutdown(wait=True)
                raise e
            else:
                logger.debug(results)
        else:
            logger.info('Invoke a trader with a streamer')
            trader.invoke()
    else:
        raise FractRuntimeError('invalid trading model')
