#!/usr/bin/env python

import logging
import os
import oandapy
from ..cli.util import read_config_yml


def close_positions(config_yml, instruments=[]):
    logger = logging.getLogger(__name__)
    logger.info('Position closing')
    cf = read_config_yml(path=config_yml)
    oanda = oandapy.API(
        environment=cf['oanda']['environment'],
        access_token=cf['oanda']['access_token']
    )

    if instruments:
        insts = set(instruments)
    else:
        pos = oanda.get_positions(account_id=cf['oanda']['account_id'])
        logger.debug('pos:{0}{1}'.format(os.linesep, pos))
        insts = set(map(lambda p: p['instrument'], pos['positions']))

    if insts:
        logger.debug('insts: {}'.format(insts))
        closed = [
            oanda.close_position(
                account_id=cf['oanda']['account_id'], instrument=i
            ) for i in insts
        ]
        logger.debug('closed:{0}{1}'.format(os.linesep, closed))
        print('All the positions closed.')
    else:
        print('No positions to close.')
