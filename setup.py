#!/usr/bin/env python

from setuptools import setup, find_packages
from fract import __version__


setup(
    name='fract',
    version=__version__,
    description='Automated Trading Framework using Oanda API',
    packages=find_packages(),
    author='Daichi Narushima',
    author_email='dnarsil+github@gmail.com',
    url='https://github.com/dceoy/fract',
    include_package_data=True,
    install_requires=[
        'docopt', 'numpy', 'pandas', 'pyyaml', 'redis', 'scikit-learn',
        'oanda-cli', 'oandapy'
    ],
    dependency_links=[
        'git+https://github.com/oanda/oandapy.git#egg=oandapy',
        'git+https://github.com/dceoy/oanda-cli.git#egg=oanda-cli'
    ],
    entry_points={
        'console_scripts': ['fract=fract.cli.main:main'],
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
        'Environment :: Console',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Topic :: Internet',
        'Topic :: Office/Business',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment'
    ],
    long_description="""\
fract
-----

Automated Trading Framework using Oanda API

This version requires Python 3 or later.
"""
)
