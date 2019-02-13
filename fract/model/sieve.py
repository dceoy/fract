#!/usr/bin/env python

import pandas as pd
import statsmodels.api as sm
from ..util.error import FractRuntimeError


def granularity2str(granularity='S5'):
    return '{0:0>2}{1:1}'.format(
        int(granularity[1:] if len(granularity) > 1 else 1), granularity[0]
    )


def select_autocorrelated_granularity(candle_dfs, method='Ljung-Box'):
    if len(candle_dfs) == 0:
        return None
    elif len(candle_dfs) == 1:
        return list(candle_dfs.keys())[0]
    elif method == 'Ljung-Box':
        return pd.concat([
            pd.DataFrame({
                'granularity': g,
                'pvalue': sm.stats.diagnostic.acorr_ljungbox(
                    x=(d['closeAsk'] + d['closeBid'])
                )[1]
            }).reset_index() for g, d in candle_dfs.items()
        ]).groupby('granularity').mean().pvalue.idxmin()
    else:
        raise FractRuntimeError('invalid method name: {}'.format(method))
