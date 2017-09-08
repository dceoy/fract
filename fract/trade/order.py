#!/usr/bin/env python

import logging
import os
import oandapy


def close_positions(config, instruments=[]):
    oanda = oandapy.API(environment=config['oanda']['environment'],
                        access_token=config['oanda']['access_token'])

    if instruments:
        insts = set(instruments)
    else:
        pos = oanda.get_positions(account_id=config['oanda']['account_id'])
        logging.debug('pos:{0}{1}'.format(os.linesep, pos))
        insts = set(map(lambda p: p['instrument'], pos['positions']))

    if insts:
        logging.debug('insts: {}'.format(insts))
        closed = [
            oanda.close_position(account_id=config['oanda']['account_id'],
                                 instrument=i)
            for i in insts
        ]
        logging.debug('closed:{0}{1}'.format(os.linesep, closed))
        print('All the positions closed.')
    else:
        print('No positions to close.')
