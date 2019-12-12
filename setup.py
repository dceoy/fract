#!/usr/bin/env python

from setuptools import find_packages, setup

from fract import __version__

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='fract',
    version=__version__,
    description='Automated Trader using Oanda V20 REST API',
    packages=find_packages(),
    author='Daichi Narushima',
    author_email='dnarsil+github@gmail.com',
    url='https://github.com/dceoy/fract',
    include_package_data=True,
    install_requires=[
        'docopt', 'numpy', 'pandas', 'pyyaml', 'redis', 'scikit-learn',
        'statsmodels', 'ujson', 'v20'
    ],
    dependency_links=[
        'git+https://github.com/dceoy/oanda-cli.git#egg=oanda-cli'
    ],
    entry_points={
        'console_scripts': ['fract=fract.cli.main:main'],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Financial and Insurance Industry',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Office/Business :: Financial :: Investment'
    ],
    python_requires='>=3.6',
    long_description=long_description
)
