#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:17:51 2020

@author: frank
"""

import pandas as pd
import pdpipe as pdp
import numpy as np
import sys
import traceback
from dateutil.relativedelta import relativedelta
from SelfTradingSystem.util.convert import (
    numberToStr, numberToDateStr, dateStrToDateTime,
    getTodayDate, getWeekNumFromDate, getMonthFromDate,
    getYearFromDate, rawStockStrToInt,
    )
from SelfTradingSystem.util.stock import (
    getStockHistory, getFundHistory, buildIndexNumberStr,
    checkMomentum, BBI, getStockHistoryV2, buildStockNumberStr
    )

class Subject:
    def __init__(self, name, sql, isStock=True):
        self.name = name
        self.isStock = isStock
        self.df = []
        self.resetFlag = False
        self.hasNewContent = False
        self.newContents = []
        self.preConditionedDF = []
        if isStock:
            self.subjectname = 'S'+name   
            self.TCloseStr = '收盘价'
            self.DateStr =  '日期'
        else:
            self.subjectname = 'F'+name
            self.TCloseStr = '累计净值'
            self.DateStr   = '净值日期'
            
        if self.subjectname == 'S000985':
            df_lastRow = sql.getLastRows('S000985', 50)
            self.validatedDate = dateStrToDateTime(numberToDateStr(int((df_lastRow.loc[0, '日期']))))
            for i in range(1, len(df_lastRow)):
                if str.isnumeric(df_lastRow.loc[i, '名称']):
                    self.validatedDate = dateStrToDateTime(numberToDateStr(int((df_lastRow.loc[i-1, '日期']))))
                    break
  
    def subjectToDF(self, sql, numRow=0):
        if numRow == 0:
            return sql.getDF(self.subjectname)
        else:
            return sql.getLastRows(self.subjectname, numRow)
    
    def setLastUpdatedDate(self, sql):
        lastRow = sql.getLastRows(self.subjectname)
        lastDate = list(lastRow[self.DateStr])[0]
        self.lastUpdatedDate = dateStrToDateTime(numberToStr(lastDate))
    
    # def writeUpdatedSubject(self, sql):
    #     if self.resetFlag:
    #         Subject.resetSubject(self, sql)
    #         self.resetFlag = False
    #         self.setLastUpdatedDate()
    #     elif self.hasNewContent:
    #         sql.updateSubject(self.subjectname,self.newContents)
    #         self.newContents = []
    #         self.hasNewContent = False
    #         self.setLastUpdatedDate()

    
    def getValidatedZZQZ(self, ):
        if self.subjectname == 'S000985':
            if self.validatedDate < self.lastUpdatedDate:
                todayDate = getTodayDate()
                startDate = todayDate - relativedelta(days=30)
                try:
                    print("Try to validateZZQZ")
                    sht_new_df = getStockHistoryV2(buildStockNumberStr('000985'), startDate, todayDate)
                    sht_new_df['日期'] = sht_new_df['日期'].apply(int)
                    sht_new_df['股票代码'] = sht_new_df['股票代码'].apply(rawStockStrToInt)
                    return sht_new_df
                    print("Finish to validateZZQZ")
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    print ("Need assisstance for unexpected error:\n {}".format(sys.exc_info()))
                    traceBackObj = sys.exc_info()[2]
                    traceback.print_tb(traceBackObj)


    
    @staticmethod                         
    def updateSubject(subObj, pool=[]):
        if subObj.hasNewContent == False:
            if subObj.resetFlag:
                subObj.sht.clear_contents()
                if subObj.isStock:
                    subObj.df = getStockHistory(subObj.name, pool=pool)
                else:
                    subObj.df = getFundHistory(subObj.name, pool=pool)
                subObj.df.loc[:, subObj.DateStr] = subObj.df.loc[:, subObj.DateStr].apply(numberToDateStr)
    #            subObj.df = subObj.df.iloc[::-1].copy().reset_index(drop=True)
    #            subObj.sht.range('A1').options(index=False).value = subObj.df
            else:
                if subObj.lastUpdatedDate < getTodayDate():
                    startDate = subObj.lastUpdatedDate
                    if subObj.isStock:
                         sht_new_df = getStockHistory(subObj.name, startDate=startDate, pool=pool)
                    else:
                         diffDays = (getTodayDate() - startDate).days
                         sht_new_df = getFundHistory(subObj.name, rows=diffDays, pool=pool)
                    sht_appended = sht_new_df[sht_new_df[subObj.DateStr].map(dateStrToDateTime) > startDate].copy()
                    sht_appended[subObj.DateStr] = sht_appended[subObj.DateStr].apply(numberToDateStr)
                    subObj.newContents = sht_appended
                    if len(subObj.newContents) > 0:
                        subObj.hasNewContent = True
                    else:
                        subObj.hasNewContent = False
        return subObj
    
    @staticmethod 
    def resetSubject(subObj, pool=[]):
        subObj.resetFlag = True
        return Subject.updateSubject(subObj, pool)
        # subObj.writeUpdatedSubject()
   
               
    def preCondition(self, sql):
        if len(self.preConditionedDF) == 0:
            numLastRow = sql.getNumRows(self.subjectname)
            maxRows = round(26*30*1.5+1)
            numRow = min([numLastRow, maxRows])
            self.subjectToDFj(numRow)
            pipeline  = pdp.ApplyByCols(self.DateStr, getWeekNumFromDate, 'Weeks', drop=False)
            pipeline += pdp.ApplyByCols(self.DateStr, getMonthFromDate, 'Month', drop=False)
            pipeline += pdp.ApplyByCols(self.DateStr, getYearFromDate, 'Year', drop=False)
            self.preConditionedDF = pipeline(self.df).copy()

    
    @staticmethod          
    def getWeekDF(df): 
        week_df_list = []
        years = df['Year'].unique()
        for year in years:
            df2 = df[df['Year'] == year]
            weeks = df2['Weeks'].unique()
            for week in weeks:
                df3 = df2[df2['Weeks'] == week]
                if len(df3) > 0:
                    week_df_list.append(df3.tail(1))
        week_df = pd.concat(week_df_list)     
        week_df = week_df.reset_index()
        return week_df
    
    @staticmethod
    def getMonthDF(df): 
        month_df_list = []
        years = df['Year'].unique()
        for year in years:
            df2 = df[df['Year'] == year]
            months = df2['Month'].unique()
            for month in months:
                df3 = df2[df2['Month'] == month]
                if len(df3) > 0:
                    month_df_list.append(df3.tail(1))
        month_df = pd.concat(month_df_list)     
        month_df = month_df.reset_index()
        return month_df
    
    @staticmethod    
    def updateMomentum(subObj):
        temp_df        = subObj.preConditionedDF
        temp_df_week   = Subject.getWeekDF(temp_df.copy())
        temp_df_month  = Subject.getMonthDF(temp_df.copy())
        dm_BBI, dm_MACD = checkMomentum(temp_df.copy())
        wm_BBI, wm_MACD  = checkMomentum(temp_df_week)
        mm_BBI, mm_MACD = checkMomentum(temp_df_month)
        m_results = [dm_BBI, dm_MACD, wm_BBI, wm_MACD, mm_BBI]
        m_results_str = [str(i) for i in m_results]
        m_results_str = "/".join(m_results_str)
        return m_results_str
    
    @staticmethod
    def getTClose(df):
        if '累计净值' in df.columns:
            TClose = df['累计净值'].values
        else:
            TClose = df['收盘价'].values
        return TClose
    
    @staticmethod
    def setTClose(df, TClose):
        if '累计净值' in df.columns:
            df['累计净值'] = TClose
        else:
            df['收盘价'] = TClose 
        return df
    
    @staticmethod    
    def updateRelativeMomentum(subObj, baseObj):
        if subObj is not baseObj:
            temp_df_sub        = subObj.preConditionedDF.copy()
            temp_df_base       = baseObj.preConditionedDF.copy()
            temp_date_sub  = dateStrToDateTime(temp_df_sub[subObj.DateStr].iloc[-1])
            temp_date_base = dateStrToDateTime(temp_df_base[baseObj.DateStr].iloc[-1])
            if temp_date_sub > temp_date_base:
                daysDiff = (temp_date_sub - temp_date_base).days - 2
                temp_df_sub = temp_df_sub.head(len(temp_df_sub)-daysDiff).copy()
            minRows = min(len(temp_df_sub), len(temp_df_base))
            temp_df_sub  = temp_df_sub.tail(minRows)
            temp_df_base = temp_df_base.tail(minRows)
            TClose_sub  = Subject.getTClose(temp_df_sub)
            TClose_base = Subject.getTClose(temp_df_base)
            TClose_final = TClose_sub/TClose_base
            temp_df  = Subject.setTClose(temp_df_sub, TClose_final)
        else:
            temp_df        = subObj.preConditionedDF
        temp_df_week   = Subject.getWeekDF(temp_df.copy())
        df_BBI = BBI(temp_df_week)
        TClose_week = Subject.getTClose(temp_df_week.tail(8))
        result_list = np.ndarray.tolist(TClose_week/df_BBI.tail(8)['BBI'].values-1)[::-1]
        temp_df_sub_week   = Subject.getWeekDF(subObj.preConditionedDF.copy())
        TClose_sub  = Subject.getTClose(temp_df_sub_week.tail(2))
        week_percent = TClose_sub[-1]/TClose_sub[-2]-1
        result_list.insert(0, week_percent)
        return result_list
    
    @staticmethod    
    def updateRelativeMomentumV2(subObj, baseObj):
        N = 12
        maxRows = 10
        temp_df_sub        = subObj.preConditionedDF.copy()
        if subObj is not baseObj:
            temp_df_base       = baseObj.preConditionedDF.copy()
#            temp_date_sub  = dateStrToDateTime(temp_df_sub[subObj.DateStr].iloc[-1])
#            temp_date_base = dateStrToDateTime(temp_df_base[baseObj.DateStr].iloc[-1])
#            if temp_date_sub > temp_date_base:
#                daysDiff = (temp_date_sub - temp_date_base).days - 2
#                temp_df_sub = temp_df_sub.head(len(temp_df_sub)-daysDiff).copy()
            minRows = min(len(temp_df_sub), len(temp_df_base), N*maxRows)
            temp_df_sub  = temp_df_sub.tail(minRows)
            temp_df_base = temp_df_base.tail(minRows)
            TClose_sub  = Subject.getTClose(temp_df_sub)
            TClose_base = Subject.getTClose(temp_df_base)
            TClose_final = TClose_sub/TClose_base
            temp_df  = Subject.setTClose(temp_df_sub.copy(), TClose_final)
        else:
            temp_df        = subObj.preConditionedDF
        lastRows= len(temp_df) - 1
        indexList = sorted(list(range(lastRows,0,-1*N)))
        
        if len(indexList) > maxRows:
            indexList = indexList[(len(indexList)-maxRows)::]
        temp_df_every_N_days = temp_df.iloc[indexList].copy().reset_index(drop=True)
#        temp_df_week   = Subject.getWeekDF(temp_df.copy())
#        df_BBI = BBI(temp_df_every_N_days)
        TClose_week = Subject.getTClose(temp_df_every_N_days.iloc[-8::])
        TClose_week_min_1 = Subject.getTClose(temp_df_every_N_days.iloc[-9:-1])
        result_list = np.ndarray.tolist(TClose_week/TClose_week_min_1-1)[::-1]
#        temp_df_sub_week   = Subject.getWeekDF(subObj.preConditionedDF.copy())
        temp_df_sub_every_N_days   = temp_df_sub.copy().iloc[indexList].copy().reset_index(drop=True)
        TClose_sub  = Subject.getTClose(temp_df_sub_every_N_days.tail(2))
        week_percent = TClose_sub[-1]/TClose_sub[-2]-1
        result_list.insert(0, week_percent)
        return result_list

if __name__=='__main__':
    from SelfTradingSystem.io.database import Database
    db = 'Resources.db'
    sql =Database(db)
    stockName = buildIndexNumberStr(985)
    obj = Subject(stockName, sql, True)
    
