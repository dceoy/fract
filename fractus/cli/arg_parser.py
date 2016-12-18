#!/usr/bin/env python

import argparse
from .. import __version__, __description__


def parse_options(config_yml):
    parser = argparse.ArgumentParser(
        prog='fract',
        description=__description__
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s {}'.format(__version__)
    )
    parser.add_argument(
        '--init',
        dest='init',
        action='store_true',
        help='Generate `{}` as a template for configuration'.format(config_yml)
    )
    parser.add_argument(
        '--debug',
        dest='debug',
        action='store_true',
        help='execute a command with debug messages'
    )
    return vars(parser.parse_args())
