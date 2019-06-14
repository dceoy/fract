#!/usr/bin/env python

import logging

from oandacli.util.config import read_yml

from ..model.kvs import RedisTrader
from ..model.standalone import StandaloneTrader


def invoke_trader(config_yml, instruments=None, model='ewma', interval_sec=0,
                  timeout_sec=3600, standalone=False, redis_host=None,
                  redis_port=6379, redis_db=0, log_dir_path=None,
                  ignore_api_error=False, quiet=False, dry_run=False):
    logger = logging.getLogger(__name__)
    logger.info('Autonomous trading')
    cf = read_yml(path=config_yml)
    if standalone:
        trader = StandaloneTrader(
            model=model, config_dict=cf, instruments=instruments,
            interval_sec=interval_sec, timeout_sec=timeout_sec,
            log_dir_path=log_dir_path, ignore_api_error=ignore_api_error,
            quiet=quiet, dry_run=False
        )
    else:
        rd = cf['redis'] if 'redis' in cf else {}
        trader = RedisTrader(
            model=model, config_dict=cf, instruments=instruments,
            redis_host=(redis_host or rd.get('host')),
            redis_port=(redis_port or rd.get('port')),
            redis_db=(redis_db if redis_db is not None else rd.get('db')),
            interval_sec=interval_sec, timeout_sec=timeout_sec,
            log_dir_path=log_dir_path, ignore_api_error=ignore_api_error,
            quiet=quiet, dry_run=False
        )
    logger.info('Invoke a trader')
    trader.invoke()
