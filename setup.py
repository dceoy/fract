#!/usr/bin/env python

from setuptools import setup, find_packages
from fract import __version__


setup(
    name='fract',
    version=__version__,
    description='Automated Trading Framework using Oanda API',
    packages=find_packages(),
    author='Daichi Narushima',
    author_email='d.narsil@gmail.com',
    url='https://github.com/dceoy/fract',
    install_requires=[
        'docopt',
        'numpy',
        'pyyaml',
        'redis',
        'oandapy',
    ],
    dependency_links=[
        'git+https://github.com/oanda/oandapy.git#egg=oandapy'
    ],
    entry_points={
        'console_scripts': ['fract=fract.cli.main:main'],
    }
)
