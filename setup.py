#!/usr/bin/env python

from setuptools import setup, find_packages
from fractus import __version__


setup(
    name='fractus',
    version=__version__,
    description='Automated Trading Framework using Oanda API',
    packages=find_packages(),
    author='Daichi Narushima',
    author_email='d.narsil@gmail.com',
    url='https://github.com/dceoy/fractus',
    install_requires=[
        'pyyaml',
        'docopt',
        'redis',
        'oandapy'
    ],
    dependency_links=[
        'git+https://github.com/oanda/oandapy.git#egg=oandapy'
    ],
    entry_points={
        'console_scripts': ['fract=fractus.cli.main:main'],
    }
)
