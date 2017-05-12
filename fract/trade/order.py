#!/usr/bin/env python

import logging
import oandapy


def close_positions(config, instruments=[]):
    oanda = oandapy.API(environment=config['oanda']['environment'],
                        access_token=config['oanda']['access_token'])

    if len(instruments) == 0:
        pos = oanda.get_positions(account_id=config['oanda']['account_id'])
        logging.debug('pos:\n{}'.format(pos))
        insts = set(map(lambda p: p['instrument'], pos['positions']))
    else:
        insts = set(instruments)

    if len(insts) > 0:
        logging.debug('insts: {}'.format(insts))
        closed = [
            oanda.close_position(account_id=config['oanda']['account_id'],
                                 instrument=i)
            for i in insts
        ]
        logging.debug('closed:\n{}'.format(closed))
        print('All the positions closed.')
    else:
        print('No positions to close.')
