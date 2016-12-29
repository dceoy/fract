#!/usr/bin/env python

import yaml


def read_yaml(path):
    with open(path) as f:
        dict = yaml.load(f)
    return dict


def print_as_yaml(dict, flow=False):
    print(yaml.dump(dict, default_flow_style=flow))
