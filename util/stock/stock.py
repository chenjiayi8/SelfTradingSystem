# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 21:49:53 2020

@author: Frank
"""

import pandas as pd
import pdpipe as pdp
from datetime import datetime
from datetime import timedelta
from multiprocessing.dummy import Pool as ThreadPool


def AMA(df, d1, SLOWSC):
    df['DIR1']= abs(df['收盘价'] - df.shift(periods=d1)['收盘价'])
    df['VIR1']= abs(df['收盘价'] - df.shift(periods=1)['收盘价'])
    df['VIR1_SUM'] = df.loc[:, 'VIR1'].rolling(window=d1).sum()
    df['ER1'] = df['DIR1']/df['VIR1_SUM']
    df['CS1'] = df['ER1']*(0.8-SLOWSC)+SLOWSC# (0.8-2/24)+2/24
    df['CQ1'] = df['CS1']*df['CS1']
    df['DMA'] = 0
    AMAName = 'AMA'+str(d1)
    df[AMAName] = 0
    d2 = 2
    for i in range(d1, len(df)):
        #X=DMA(C,A)=A*X+(1-A)*X'(A小于1）
        #Y=EMA(X,N)=［2*X+(N-1)*Y’］/(N+1)，
        if i == d1:
            df.loc[i, 'DMA'] = df.loc[i, '收盘价']
            df.loc[i, AMAName] = df.loc[i, 'DMA']
        else:
            df.loc[i, 'DMA'] = df.loc[i, 'CQ1']*df.loc[i, '收盘价'] + (1-df.loc[i, 'CQ1'])*df.loc[i-1, 'DMA']
            df.loc[i, AMAName] = (2*df.loc[i, 'DMA'] + (d2-1)*df.loc[i-1, AMAName])/(d2+1)
    
    return df.drop(['DIR1', 'VIR1', 'VIR1_SUM', 'ER1', 'CS1', 'CQ1', 'DMA'], axis=1)

def AMAs(df):
    d1 = 5
    d2 = 10
    df2 = AMA(df, d1, 2/8)
    df3 = AMA(df2, d2, 2/24)
    return df3

def MA(TClose, num):
    MA = []
    TClose = TClose[::-1]
    numLastRow = len(TClose)
    for i in range(numLastRow):
        if i + num < numLastRow:
            MA.append(sum(TClose[i:i+num])/num)
        else:
            MA.append(0.0)
    MA = MA[::-1]
    return MA

def MA_pool_wrapper(args):
    return MA(*args)

def MAs(df, nums): #moving average 
    if '累计净值' in df.columns:
        TClose = df['累计净值'].values
    else:
        TClose = df['收盘价'].values
        
    inputs = []
    for num in nums:
        inputs.append([TClose, num])
      
    temp_pool = ThreadPool(len(nums))
    MAs = temp_pool.map(MA_pool_wrapper, inputs)
     
    for i in range(len(nums)):
        df['MA'+str(nums[i])] = MAs[i]
    
#    for i in range(len(nums)):
#        MA_Temp = MA(TClose, nums[i])
#        df['MA'+str(nums[i])] = MA_Temp
    return df
        
def BBI(df):
    intervals = [3,6,12,24]
    df = MAs(df, intervals)
    df.loc[:,'BBI'] = 0
    for interval in intervals:
        df.loc[:, 'BBI'] += df.loc[:, 'MA'+str(interval)]
    df.loc[:, 'BBI'] /=len(intervals)
    df.loc[df['MA'+str(max(intervals))] == 0, 'BBI'] = 0
    df.drop(list(df.filter(regex = 'MA')), axis = 1, inplace = True)
    return df

def MACD(df):
    if '累计净值' in df.columns:
        TCloseStr = '累计净值'
    else:
        TCloseStr = '收盘价'
    intervals = [9, 12, 26]
    smoothFacotr1 = 2/(intervals[1]+1)
    smoothFacotr2 = 2/(intervals[2]+1)
    smoothFacotr0 = 2/(intervals[0]+1)
    df.loc[:,'EMA'+str(intervals[1])] = 0
    df.loc[:,'EMA'+str(intervals[2])] = 0
    df.loc[:,'DIFF'] = 0
    df.loc[:,'DEA'] = 0
    df.loc[:,'BAR'] = 0
    for i in range(intervals[1]-1, len(df)):
        df.loc[i,'EMA'+str(intervals[1]) ] = df.loc[i-1,'EMA'+str(intervals[1]) ]*(1-smoothFacotr1) + df.loc[i, TCloseStr]*smoothFacotr1
    for i in range(intervals[2]-1, len(df)):
        df.loc[i,'EMA'+str(intervals[2]) ] = df.loc[i-1,'EMA'+str(intervals[2]) ]*(1-smoothFacotr2) + df.loc[i, TCloseStr]*smoothFacotr2
    df.loc[:,'DIFF']  = df.loc[:,'EMA'+str(intervals[1])] -  df.loc[:,'EMA'+str(intervals[2])]
    for i in range(intervals[2]-1, len(df)):
        df.loc[i,'DEA'] = df.loc[i-1,'DEA']*(1-smoothFacotr0) + df.loc[i,'DIFF']*smoothFacotr0
    df.loc[:,'BAR'] = 2*(df.loc[:,'DIFF'] - df.loc[:,'DEA'])
    df = df.drop(['EMA'+str(intervals[1]), 'EMA'+str(intervals[2]), 'DIFF', 'DEA'], axis=1)
    return df, TCloseStr



if __name__ == '__main__':
    numDays = 10
    todayDate = datetime.today()
    dates = [todayDate+timedelta(days=-1*i) for i in range(numDays)]
    dates = dates[::-1]
    TClose = list(range(10))
    df = pd.DataFrame(zip(dates, TClose), columns=['日期', '收盘价'])
    MA3 = MA(TClose, 3)
