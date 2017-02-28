#!/usr/bin/env python

import yaml


def read_yaml(path):
    with open(path) as f:
        d = yaml.load(f)
    return d


def print_as_yaml(dict, flow=False):
    print(yaml.dump(dict, default_flow_style=flow))
