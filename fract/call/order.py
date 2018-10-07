#!/usr/bin/env python

import logging
import os
import oandapy
from ..util.config import read_config_yml


def close_positions(config_yml, instruments=[]):
    logger = logging.getLogger(__name__)
    logger.info('Position closing')
    cr = read_config_yml(path=config_yml)['oanda']
    oanda = oandapy.API(
        environment=cr['environment'], access_token=cr['access_token']
    )
    insts = {
        p['instrument'] for p in
        oanda.get_positions(account_id=cr['account_id'])['positions']
        if not instruments or p['instrument'] in instruments
    }
    if insts:
        logger.debug('insts: {}'.format(insts))
        closed = [
            oanda.close_position(account_id=cr['account_id'], instrument=i)
            for i in insts
        ]
        logger.debug('closed:{0}{1}'.format(os.linesep, closed))
        print('All the positions closed.')
    else:
        print('No positions to close.')
