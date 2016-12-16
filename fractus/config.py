#!/usr/bin/env python

import yaml


def read_yaml(yml):
    with open(yml) as f:
        dict = yaml.load(f)
    return dict
