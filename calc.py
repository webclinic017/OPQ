

'''
Functions to calculate the metrics for the investing system.

'''

import pandas as pd
import numpy as np
import scipy.stats as st




def preprocess(stock_x):
    '''
    For the given stock, calculate its:
        - 3-day Simple Moving Average (SMA3), SMA3 rank, SMA3 normalized
        - Log Daily Return
        - Close price rank, close price normalized
    '''
    
    stock_x['SMA3'] = stock_x['CLOSE'].rolling(window=3).mean()
    
    stock_x['log_return'] = np.log(stock_x['CLOSE']) - np.log(stock_x['CLOSE'].shift(1))

    CLOSE_mean = stock_x['CLOSE'].mean()
    CLOSE_std = stock_x['CLOSE'].std()
    stock_x['CLOSE_normalized'] = (stock_x['CLOSE'] - CLOSE_mean) / CLOSE_std

    SMA3_mean = stock_x['SMA3'].mean()
    SMA3_std = stock_x['SMA3'].std()
    stock_x['SMA3_normalized'] = (stock_x['SMA3'] - SMA3_mean) / SMA3_std

    stock_x['CLOSE_rank'] = stock_x['CLOSE'].rank()
    
    stock_x['SMA3_rank'] = stock_x['SMA3'].rank()

    return stock_x




def calc_PCC_raw(stock_x, stock_y):
    '''
    Pearson Correlation Coefficient using raw close price
    '''
    
    df = pd.DataFrame(columns=['X', 'Y'])
    
    df['X'] = stock_x['CLOSE']
    df['Y'] = stock_y['CLOSE']
    
    return df.corr()['X']['Y']



def calc_PCC_SMA3(stock_x, stock_y):
    '''
    Pearson Correlation Coefficient using 3-day simple moving average of close price
    '''
    
    df = pd.DataFrame(columns=['X', 'Y'])
    
    df['X'] = stock_x['SMA3']
    df['Y'] = stock_y['SMA3']
    
    return df.corr()['X']['Y']


def calc_PCC_log(stock_x, stock_y):
    '''
    Pearson Correlation Coefficient using daily log return
    '''
    
    df = pd.DataFrame(columns=['X', 'Y'])
    
    df['X'] = stock_x['log_return']
    df['Y'] = stock_y['log_return']
    
    return df.corr()['X']['Y']


def calc_SCC_raw(stock_x, stock_y):
    '''
    Spearman's Correlation Coefficient using close price's rank
    '''
    
    df = pd.DataFrame(columns=['diff_sq'])
    
    df['diff_sq'] = (stock_x['CLOSE_rank'] - stock_y['CLOSE_rank']) ** 2
    df['diff_sq'] = df['diff_sq'].dropna()
    n = len(df['diff_sq'])

    return 1 - ( 6 * df['diff_sq'].sum() / (n**3 - n) )


def calc_SCC_SMA3(stock_x, stock_y):
    '''
    Spearman's Correlation Coefficient using SMA3's rank
    '''
    
    df = pd.DataFrame(columns=['diff_sq'])
    
    df['diff_sq'] = (stock_x['SMA3_rank'] - stock_y['SMA3_rank']) ** 2
    df['diff_sq'] = df['diff_sq'].dropna()
    n = len(df['diff_sq'])

    return 1 - ( 6 * df['diff_sq'].sum() / (n**3 - n) )


def calc_SSD_raw(stock_x, stock_y):
    '''
    Average of Squared Differences using normalized close price
    '''
    
    df = pd.DataFrame(columns=['diff_sq'])

    df['diff_sq'] = (stock_x['CLOSE_normalized'] - stock_y['CLOSE_normalized']) ** 2
    df['diff_sq'] = df['diff_sq'].dropna()
    
    return df['diff_sq'].mean()


def calc_SSD_SMA3(stock_x, stock_y):
    '''
    Average of Squared Differences using normalized SMA3
    '''
    
    df = pd.DataFrame(columns=['diff_sq'])

    df['diff_sq'] = (stock_x['SMA3_normalized'] - stock_y['SMA3_normalized']) ** 2
    df['diff_sq'] = df['diff_sq'].dropna()
    
    return df['diff_sq'].mean()


def calc_CoInt(stock_x, stock_y):
    '''
    Simplified cointegration method. Assume that
        Y - beta * X = u
    and u is "stationary".
    Find the beta, Coeffecient or Variation of u and R-squared of the linear regression.
    '''

    df = stock_x.join(stock_y, how="inner", lsuffix="_X", rsuffix="_Y").dropna()

    linreg = st.linregress(df['CLOSE_X'], df['CLOSE_Y'])
    beta = linreg.slope
    rsq = linreg.rvalue

    return {
        "beta": linreg.slope,
        "alpha": linreg.intercept,
        "rsq": linreg.rvalue,
        "pvalue": linreg.pvalue,
        "stderr": linreg.stderr
    }




    


