#!/usr/bin/env python

from setuptools import setup, find_packages
from fractus import __version__, __description__


setup(
    name='fractus',
    version=__version__,
    description=__description__,
    packages=find_packages(),
    author='Daichi Narushima',
    author_email='d.narsil@gmail.com',
    url='https://github.com/dceoy/fractus',
    install_requires=[
        'redis'
    ],
    dependency_links=[
        'http://github.com/oanda/oandapy/tarball/master#egg=python_dateutil'
    ],
    entry_points={
        'console_scripts': ['fract=fractus.cli.main:main'],
    }
)
