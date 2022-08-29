#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:17:51 2020

@author: frank
"""

import pandas as pd
import pdpipe as pdp
import numpy as np
from datetime import datetime
import sys
import traceback
from dateutil.relativedelta import relativedelta
from SelfTradingSystem.util.convert import (
    numberToStr, numberToDateStr, dateStrToDateTime,
    getTodayDate, getWeekNumFromDate, getMonthFromDate,
    getYearFromDate, rawStockStrToInt, rawTextToNumeric, dateTimeToDateStr,
    )
from SelfTradingSystem.util.stock import (
    getStockHistory, buildIndexNumberStr, getFundHistoryFromSinaBetween,
    checkMomentum, BBI, getStockHistoryV2, buildStockNumberStr,
    getWholeFundHistoryFromSina, getStockHistroyFromCSINDEX
    )

from SelfTradingSystem.util.others import debugger, readBug

def updateRelativeMomentumWrapper(input_instance):
    return Subject.updateRelativeMomentumV2(*input_instance)

class Subject:
    def __init__(self, name, sql, isStock=True):
        self.name = name
        self.isStock = isStock
        self.resetFlag = False
        self.hasNewContent = False
        self.newContents = []
        self.preConditionedDF = []
        self.SZIndexs = ['S000170', 'S399372', 'S399373', 'S399374',
                         'S399375', 'S399376', 'S399377', 'S399932',
                         'S399006', 'S399300']
        if isStock:
            self.subjectname = 'S'+name   
            self.TCloseStr = '收盘价'
            self.DateStr =  '日期'
        else:
            self.subjectname = 'F'+name
            self.TCloseStr = '累计净值'
            self.DateStr   = '净值日期'
            
        # if self.subjectname == 'S000985':
        #     self.setValidatedDate(sql)
  
    def subjectToDF(self, sql, numRow=0):
        if numRow == 0:
            return sql.getDF(self.subjectname)
        else:
            return sql.getLastRows(self.subjectname, numRow)
    
    def setValidatedDate(self, sql):
        assert self.subjectname == 'S000985', "Target subject is not S000985"
        df_lastRow = sql.getLastRows('S000985', 50)
        self.validatedDate = dateStrToDateTime(numberToDateStr(int((df_lastRow.loc[0, '日期']))))
        for i in range(1, len(df_lastRow)):
            if not str.isnumeric(df_lastRow.loc[i, '名称']):
                # self.validatedDate = dateStrToDateTime(numberToDateStr(int((df_lastRow.loc[i-1, '日期']))))
            # else:
                self.validatedDate = dateStrToDateTime(numberToDateStr(int((df_lastRow.loc[i, '日期']))))
        
    
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

    
    def getValidatedZZQZ(self):
        assert self.subjectname == 'S000985', "Target subject is not S000985"
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
    def updateSubject(subobj, pool=[]):
        try:
            if subobj.hasNewContent == False:
                if subobj.resetFlag:
                    if subobj.isStock:
                        df = getStockHistory(subobj.name, pool=pool)
                        # df = getStockHistoryV2(subobj.name)
                    else:
                        # df = getFundHistory(subobj.name, pool=pool)
                        df = getWholeFundHistoryFromSina(subobj.name, pool=pool)
                    df.loc[:, subobj.DateStr] = df.loc[:, subobj.DateStr].apply(numberToDateStr)
                    subobj.newContents = df
        #            subobj.df = subobj.df.iloc[::-1].copy().reset_index(drop=True)
        #            subobj.sht.range('A1').options(index=False).value = subobj.df
                else:
                    todayDate = getTodayDate()
                    if subobj.lastUpdatedDate < todayDate:
                        startDate = subobj.lastUpdatedDate
                        if subobj.isStock:
                            # if subobj.subjectname in subobj.SZIndexs:
                            #     sht_new_df = getStockHistory(subobj.name, startDate=startDate)
                            # else:
                                # sht_new_df = getStockHistroyFromCSINDEX(subobj.name, startDate=startDate)
                            #     sht_new_df = getStockHistory(subobj.name, startDate=startDate)
                            
                            if subobj.subjectname in ['S000985', 'S399995', 'S000989']:
                                # print("Updating {} from {}".format(subobj.subjectname, dateTimeToDateStr(subobj.lastUpdatedDate)))
                                sht_new_df = getStockHistroyFromCSINDEX(subobj.name, startDate=startDate, endDate=todayDate) 
                                # sht_new_df = getStockFromInvest(subobj.name, startDate=startDate)
                            # elif subobj.subjectname in ['S399995', 'S000989']:
                                # sht_new_df = getStockHistoryV2(subobj.name, startDate=startDate)
                            else:
                                # sht_new_df = getStockHistoryV2(subobj.name, startDate=startDate)
                                sht_new_df = getStockHistory(subobj.name, startDate=startDate, endDate=todayDate, pool=pool)
                                
                        else:
                             # diffDays = (getTodayDate() - startDate).days
                            datefrom = datetime.strftime(startDate- relativedelta(days=10), "%Y-%m-%d")
                            dateto   = datetime.strftime(datetime.today(), "%Y-%m-%d")
                            sht_new_df = getFundHistoryFromSinaBetween(subobj.name, datefrom, dateto, pool=pool)
                                # sht_new_df = getFundHistory(subobj.name, rows=diffDays, pool=pool)
                        if len(sht_new_df) > 0:  
                            sht_appended = sht_new_df[sht_new_df[subobj.DateStr].map(dateStrToDateTime) > startDate].copy()
                            sht_appended[subobj.DateStr] = sht_appended[subobj.DateStr].apply(numberToDateStr)
                            if len(sht_appended) > 0:
                                subobj.newContents = sht_appended
                            else:
                                if len(sht_new_df) > 1 and subobj.subjectname in ['S000985', 'S399995', 'S000989']:
                                    debugger([subobj, startDate, sht_new_df, sht_appended], subobj.subjectname)
                                    raise ValueError('Bug found when updating {}'.format(subobj.subjectname))
                                   
                                
                if len(subobj.newContents) > 0:
                    subobj.hasNewContent = True
                    if subobj.isStock:
                        targetColumns = ['收盘价', '成交量(股)','成交金额(元)',\
                                         '开盘价','日期', '涨跌幅(%)',\
                                         '最低价','涨跌额','涨跌幅','最高价']
                    else:
                        targetColumns = ['单位净值', '累计净值', '日增长率']
                    pipeline  = pdp.ApplyByCols(targetColumns[0], rawTextToNumeric, targetColumns[0], drop=False)
                    for column in targetColumns[1:]:
                        if column in subobj.newContents.columns:
                            pipeline  += pdp.ApplyByCols(column, rawTextToNumeric, column, drop=False)
                    # if subobj.subjectname == 'S000985':
                        # pipeline  += pdp.ApplyByCols('股票代码', rawStockStrToInt, '股票代码', drop=False)
                    subobj.newContents = pipeline(subobj.newContents)
                    if subobj.subjectname in ['S000985', 'S399995', 'S000989']:
                        print("{} has new content after {}".format(subobj.subjectname, dateTimeToDateStr(subobj.lastUpdatedDate)))
                else:
                    subobj.hasNewContent = False
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print ("Need assisstance at {} for unexpected error:\n {}".format(subobj.subjectname, sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
        return subobj
    
    @staticmethod 
    def resetSubject(subobj, pool=[]):
        subobj.resetFlag = True
        return Subject.updateSubject(subobj, pool)
        # subobj.writeUpdatedSubject()
   
    def subjectToDFj(self, numRow, sql):
        return sql.getLastRows(self.subjectname, numRow)
    
    
    def preCondition(self, sql, maxRows=round(26*30*1.5+1)):
        if len(self.preConditionedDF) == 0:
            numLastRow = sql.getNumRows(self.subjectname)
            numRow = min([numLastRow, maxRows])
            df = self.subjectToDFj(numRow, sql)
            df[self.TCloseStr] = df[self.TCloseStr].fillna(method='ffill') #remove Nan of QDII in price history
            pipeline  = pdp.ApplyByCols(self.DateStr, getWeekNumFromDate, 'Weeks', drop=False)
            pipeline += pdp.ApplyByCols(self.DateStr, getMonthFromDate, 'Month', drop=False)
            pipeline += pdp.ApplyByCols(self.DateStr, getYearFromDate, 'Year', drop=False)
            pipeline += pdp.ApplyByCols(self.TCloseStr, float)
            try:
                self.preConditionedDF = pipeline(df).copy()
            except:
                print("Error at {}".format(self.subjectname))
                pass

    
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
    def updateMomentum(subobj):
        try:
            temp_df        = subobj.preConditionedDF
            temp_df_week   = Subject.getWeekDF(temp_df.copy())
            temp_df_month  = Subject.getMonthDF(temp_df.copy())
            dm_BBI, dm_MACD = checkMomentum(temp_df.copy())
            wm_BBI, wm_MACD  = checkMomentum(temp_df_week)
            mm_BBI, mm_MACD = checkMomentum(temp_df_month)
            m_results = [dm_BBI, dm_MACD, wm_BBI, wm_MACD, mm_BBI]
            m_results_str = [str(i) for i in m_results]
            m_results_str = "/".join(m_results_str)
            temp_df_52weeks = temp_df_week.tail(52)
            TCloses_52weeks = temp_df_52weeks[subobj.TCloseStr]
            max_52weeks = TCloses_52weeks.max()
            min_52weeks = TCloses_52weeks.min()
            position = (TCloses_52weeks.iloc[-1] - min_52weeks)/(max_52weeks-min_52weeks)
            currentDraw = 1-TCloses_52weeks.iloc[-1]/max_52weeks
            position = '{:.2%}/{:.2%}'.format(position, currentDraw)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print("Need assistance for unexpected error at {}:\n {}"
                       .format(subobj.name, sys.exc_info()))
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            m_results_str = ''
            position = ''
        return m_results_str, position
    
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
    def updateRelativeMomentum(subobj, baseObj):
        try:
            if subobj is not baseObj:
                temp_df_sub        = subobj.preConditionedDF.copy()
                temp_df_base       = baseObj.preConditionedDF.copy()
                temp_date_sub  = dateStrToDateTime(temp_df_sub[subobj.DateStr].iloc[-1])
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
                temp_df        = subobj.preConditionedDF
            temp_df_week   = Subject.getWeekDF(temp_df.copy())
            df_BBI = BBI(temp_df_week)
            TClose_week = Subject.getTClose(temp_df_week.tail(8))
            result_list = np.ndarray.tolist(TClose_week/df_BBI.tail(8)['BBI'].values-1)[::-1]
            temp_df_sub_week   = Subject.getWeekDF(subobj.preConditionedDF.copy())
            TClose_sub  = Subject.getTClose(temp_df_sub_week.tail(2))
            week_percent = TClose_sub[-1]/TClose_sub[-2]-1
            result_list.insert(0, week_percent)
        except:
            print("updateRelativeMomentum error at {}".format(subobj.name))
            result_list = list(np.zeros((1, 9), dtype=float))
        return result_list
    
    @staticmethod    
    def updateRelativeMomentumV2(subobj, baseObj):
        N = 12
        maxRows = 10
        try:
            temp_df_sub        = subobj.preConditionedDF.copy()
            if subobj is not baseObj:
                temp_df_base       = baseObj.preConditionedDF.copy()
    #            temp_date_sub  = dateStrToDateTime(temp_df_sub[subobj.DateStr].iloc[-1])
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
                temp_df        = subobj.preConditionedDF
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
    #        temp_df_sub_week   = Subject.getWeekDF(subobj.preConditionedDF.copy())
            temp_df_sub_every_N_days   = temp_df_sub.copy().iloc[indexList].copy().reset_index(drop=True)
            TClose_sub  = Subject.getTClose(temp_df_sub_every_N_days.tail(2))
            week_percent = TClose_sub[-1]/TClose_sub[-2]-1
            result_list.insert(0, week_percent)
            result_list = ["{0:.2f}%".format(result*100) for result in result_list]
        except:
            print("updateRelativeMomentumV2 error at {}".format(subobj.name))
            result_list = list(np.zeros((1, 9), dtype=float))
        return result_list

if __name__=='__main__':
    from SelfTradingSystem.io.database import Database
    db = 'Resources.db'
    sql =Database(db)
    # stockName = buildIndexNumberStr(985)
    # subobj = Subject('159915', sql, False)
    
