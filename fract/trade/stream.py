#!/usr/bin/env python

import logging
import json
import signal
import oandapy
import redis


class StreamDriver(oandapy.Streamer):
    def __init__(self, stream_type, redis_config, **kwargs):
        super().__init__(**kwargs)
        self.type = stream_type
        self.key = {'rate': 'tick', 'event': 'transaction'}[self.type]
        if redis_config is None:
            logging.debug('Set a streamer')
            self.redis = None
        else:
            logging.debug('Set a streamer with Redis')
            self.redis = redis.StrictRedis(host=redis_config['ip'],
                                           port=redis_config['port'],
                                           db=redis_config['db'])
            self.redis_max = redis_config['max_llen']
            self.redis.flushdb()

    def on_success(self, data):
        print(data)
        if self.redis is not None:
            instrument = data[self.key]['instrument']
            self.redis.rpush(instrument, json.dumps(data))
            if self.redis.llen(instrument) > self.redis_max:
                self.redis.lpop(instrument)

    def on_error(self, data):
        logging.error(data)
        self.disconnect()

    def fire(self, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if self.type == 'rate':
            logging.debug('Start to stream market prices')
            self.rates(**kwargs)
        elif self.type == 'event':
            logging.debug('Start to stream authorized account\'s events')
            self.events(**kwargs)


def invoke(stream_type, instruments, config, redis_config):
    insts = (instruments if instruments else config['trade']['instruments'])
    stream = StreamDriver(stream_type=stream_type,
                          environment=config['oanda']['environment'],
                          access_token=config['oanda']['access_token'],
                          redis_config=redis_config)
    stream.fire(account_id=config['oanda']['account_id'],
                instruments=','.join(insts),
                ignore_heartbeat=True)
