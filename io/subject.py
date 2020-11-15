#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:17:51 2020

@author: frank
"""

import pandas as pd
import pdpipe as pdp
import numpy as np
from SelfTradingSystem.util.convert import (
    numberToStr, numberToDateStr, dateStrToDateTime,
    getTodayDate, getWeekNumFromDate, getMonthFromDate,
    getYearFromDate,
    )
from SelfTradingSystem.util.stock import (
    getStockHistory, getFundHistory, buildStockNumberStr,
    checkMomentum, BBI,
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
            self.sheetName = 'S'+name   
            self.TCloseStr = '收盘价'
            self.DateStr =  '日期'
        else:
            self.sheetName = 'F'+name
            self.TCloseStr = '累计净值'
            self.DateStr   = '净值日期'

        self.setLastUpdatedDateStr(sql)
  
    def subjectToDF(self, sql, numRow=0):
        if numRow == 0:
            return sql.getDF(self.sheetName)
        else:
            return sql.getLastRows(self.sheetName, numRow)
    
    def setLastUpdatedDateStr(self, sql):
        lastRow = sql.getLastRows(self.sheetName)
        lastDate = list(lastRow[self.DateStr])[0]
        self.lastUpdateDateStr = numberToStr(lastDate)
    
    def writeUpdatedSheet(self, sql):
        if self.resetFlag:
            self.sht.range('A1').options(index=False).value = self.df
            self.resetFlag = False
            self.setLastUpdatedDateStr()
        elif self.hasNewContent:
            numLastRow = self.sht.range('A1').current_region.last_cell.row
            self.sht.range(numLastRow+1, 1).value = self.newContents
            self.newContents = []
            self.hasNewContent = False
            self.setLastUpdatedDateStr()

    

    @staticmethod                         
    def updateSheet(subObj, pool=[]):
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
                startDate=dateStrToDateTime(subObj.lastUpdateDateStr)  
                if startDate < getTodayDate():
                    if subObj.isStock:
                         sht_new_df = getStockHistory(subObj.name, startDate=startDate, pool=pool)
                    else:
                         diffDays = (getTodayDate() - startDate).days
                         sht_new_df = getFundHistory(subObj.name, rows=diffDays, pool=pool)
                    sht_appended = sht_new_df[sht_new_df[subObj.DateStr].map(dateStrToDateTime) > startDate].copy()
    #                sht_appended.loc[:, self.DateStr] = sht_appended.loc[:, self.DateStr].apply(numberToDateStr)
                    sht_appended[subObj.DateStr] = sht_appended[subObj.DateStr].apply(numberToDateStr)
    #                sht_appended = sht_appended[sht_appended[self.DateStr].apply(numberToDateStr)]
    #                self.df = pd.concat([self.df, sht_appended], sort=False)
    #                self.df = self.df.reset_index()
                    subObj.newContents = sht_appended.values.tolist()
                    if len(subObj.newContents) > 0:
                        subObj.hasNewContent = True
                    else:
                        subObj.hasNewContent = False
#                subObj.sht.range(numLastRow+1, 1).value = 
    
    @staticmethod 
    def resetSheet(subObj, pool=[]):
        subObj.resetFlag = True
        Subject.updateSheet(subObj, pool)
        subObj.writeUpdatedSheet()
   
               
    def preCondition(self, sql):
        if len(self.preConditionedDF) == 0:
            numLastRow = sql.getNumRows(self.sheetName)
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
    stockName = '000985'
    obj = Subject(stockName, sql, True)
