#!/usr/bin/env python

from setuptools import find_packages, setup

from fract import __version__

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='fract',
    version=__version__,
    author='Daichi Narushima',
    author_email='dnarsil+github@gmail.com',
    description='Automated Trader using Oanda V20 REST API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/dceoy/fract',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'docopt', 'numpy', 'oanda-cli', 'pandas', 'pyyaml', 'redis',
        'scikit-learn', 'statsmodels', 'ujson', 'v20'
    ],
    entry_points={'console_scripts': ['fract=fract.cli.main:main']},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Financial and Insurance Industry',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 3',
        'Topic :: Office/Business :: Financial :: Investment'
    ],
    python_requires='>=3.6'
)
