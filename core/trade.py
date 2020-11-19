# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 08:25:32 2019

@author: Frank
This is a Class file for Subject:
variables:
1. TClosStr
2. sht pointer
3. df pointer

methods:
1. update stocks/funds
2. update stock during trading hours
3. calculate week / month momentum
4. write momentums to sheet menu for further usage

"""
#import modin.pandas as pd
import sys
sys.path.append('D:\\LinuxWorkFolder\\TUD\\Python\\Library')

import os
# from addLibraries import Helper
import xlwings as xw
from lxml import html
import time
from datetime import datetime
from datetime import timedelta
#import pytz
#from dateutil.relativedelta import relativedelta
# import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
# import re
# import math
import itertools
import pandas as pd
import pdpipe as pdp
#import autopy
import subprocess
# from socket import timeout
import numpy as np
#from addLibraries import Helper
from inspect import currentframe, getframeinfo
from pandas.core.common import SettingWithCopyError
pd.options.mode.chained_assignment = 'raise'
from dateutil.relativedelta import relativedelta
# import ExcelHelper
import tabulate as tb
# from SmartQ_PythonV2 import SmartQ_Python, dfToImg
from SelfTradingSystem.io.subject import Subject, updateRelativeMomentumWrapper
from SelfTradingSystem.util.stock import (
    getStock, getFund, getStocks, getHTML,
    BBI, MACD, MAs, AMAs,
    )
from SelfTradingSystem.util.others import (
    round_up, round_down, getLastTradedTime,
    isnumeric, mergeImg, sleep
    )
from SelfTradingSystem.io.excel import (
    sheetToDF, indCell, getColumnStr, getTargetArea,
    writeTradedTasks, writeMomentTasks,
    )     

from SelfTradingSystem.util.convert import (
    numberToStr, numberToDateStr, dateStrToDateTime,
    getTodayDate, getWeekNumFromDate, getMonthFromDate,
    getYearFromDate, rawStockStrToInt, rawTextToNumeric,
    getStockNumberStr, dateTimeToDateStr, getTomorrowDateStr,
    getTodayDateStr, getDeltaDateStr, getNowTimeStr,
    getDaysBetweenDateStrs, getTomorrowDate
    )
from SelfTradingSystem.core.strategy import (
    calculateForMomentumShare,
)
from SelfTradingSystem.util.remindMe import sendEmail
from SelfTradingSystem.io.database import Database

class Trade:
    def __init__(self, xlsxName, sql):
        self.xlsxName = xlsxName
        self.sql = sql
        self.wb = xw.Book(xlsxName)
        self.pool = ThreadPool(20)
        self.msg = []
        self.imgs = []
        self.imgPath = ''
        self.sheetNames = [self.wb.sheets[s].name for s in range(self.wb.sheets.count)]
        self.shf_config = self.wb.sheets['Menu']
        self.s_Str = 'B'
        self.f_str = 'F'
        self.row_num = 12
        self.numRowLastStock = self.shf_config.range(indCell(self.s_Str, 1)).current_region.last_cell.row
        self.numRowLastFund = self.shf_config.range(indCell(self.f_str, 1)).current_region.last_cell.row
        self.momentumMaps = []
        for r in range(12,self.numRowLastStock+1):
            if self.shf_config.range(indCell(self.s_Str, r)).value != None:
                self.momentumMaps.append(chr(ord(self.s_Str)+2)+str(r))
        for r in range(12,self.numRowLastFund+1):
            if self.shf_config.range(indCell(self.f_str, r)).value != None:
                self.momentumMaps.append(chr(ord(self.f_str)+2)+str(r))
        self.stockNumberStrs = [numberToStr(self.shf_config.range(indCell(self.s_Str, r)).value) for r in range(12, self.numRowLastStock+1) if self.shf_config.range(indCell(self.s_Str, r)).value != None ]
        self.fundNumberStrs = [numberToStr(self.shf_config.range(indCell(self.f_str, r)).value) for r in range(12, self.numRowLastFund+1) if self.shf_config.range(indCell(self.f_str, r)).value != None ]
        

    def initialSubjects(self):
        self.tradeObjs = self.sql.objMap.items()
        self.objMap = self.sql.objMap
            
    def atWeekend(self):
        nowTime = time.localtime()
        flag = False
        if nowTime.tm_wday == 4 and nowTime.tm_hour >= 16:
            flag =  True
        if nowTime.tm_wday in [5, 6] :
            flag =  True
        return flag

    
    def atTradingTime(self):
#        currentTimeInChina = datetime.now(pytz.timezone('Asia/Chongqing')).timetuple()
        mainIndex = getStock('sh510050')
        currentTradingTimeStr = mainIndex[31]
        currentTradingTime = datetime.strptime(currentTradingTimeStr, "%H:%M:%S").timetuple()
        if currentTradingTime.tm_hour > 9 and currentTradingTime.tm_hour < 15:
            return True
        else:
            return False



        
    def updateMomentums(self):
        for obj in self.tradeObjs:
            obj.preCondition()
        momentum_results = self.pool.map(Subject.updateMomentum, self.tradeObjs)
#        self.momentum_results = []
        for i in range(len(self.tradeObjs)):
#            self.momentum_results.append(Subject.updateMomentum(self.tradeObjs[i]))
            self.shf_config.range(self.momentumMaps[i]).value = momentum_results[i]
            
    def updateRelativeMomentums(self):
        for obj in self.tradeObjs:
            obj.preCondition()
        baseObj = self.objMap['S000985']
        inputs = [[subObj, baseObj] for subObj in self.tradeObjs]
        momentum_results = self.pool.map(updateRelativeMomentumWrapper, inputs)
#        momentum_results = []
        shf = self.wb.sheets['趋势']
        for i in range(len(self.tradeObjs)):
#            momentum_results.append(Subject.updateRelativeMomentumV2(self.tradeObjs[i], baseObj))
            shf.range('V'+str(i+2)).value = momentum_results[i]      
            
    def updateStockSheetLive(self):
        sht_stockInfo = self.wb.sheets['股票查询']
        numRowsStock = sht_stockInfo.range('A1').current_region.last_cell.row
        endRowStr = 'A'+str(numRowsStock)
        endRowStr2 = 'C'+str(numRowsStock)
        tasks = sht_stockInfo.range('A2', endRowStr).value
#        results = []
#        for task in tasks:
#            results.append(getStock(task))
        
#        results = self.pool.map(getStock, tasks)
        results = getStocks(tasks)
        sht_stockInfo.range('C2', endRowStr2).value = results
        updatedTime =  [[time.strftime("%d/%m/%Y, %H:%M:%S")]]*(numRowsStock-1)
        sht_stockInfo.range('AJ2').value = updatedTime      
            
    def updateFundSheetLive(self):
        sht_fundInfo = self.wb.sheets['净值查询']
        numRows = sht_fundInfo.range('A1').current_region.last_cell.row
        endRowStr = 'A'+str(numRows)
        tasks = sht_fundInfo.range('A2', endRowStr).value
        tasks = [numberToStr(task) for task in tasks]
#        results = []
#        for task in tasks:
#            results.append(getFund(task))
        results = self.pool.map(getFund, tasks)
        results = list(itertools.chain(*results))
        for i in range(2, numRows+1):
            if len(results[i-2]) > 3:
                sht_fundInfo.range('C'+str(i)).value = results[i-2]
                sht_fundInfo.range('J'+str(i)).value = time.strftime("%d/%m/%Y, %H:%M:%S")
        
    def getMomentumTargets(self):
        sht = self.wb.sheets['趋势']
        numLastRow_Buy_Lock = sht.range('O1').current_region.last_cell.row
        numLastRow_Sell_Lock = sht.range('P1').current_region.last_cell.row
        buyLockedTargets = sht.range('O2', 'O'+str(numLastRow_Buy_Lock)).value
        buyLockedTargets = [target for target in buyLockedTargets if len(target) > 0]
        sellLockedTargets = sht.range('P2', 'P'+str(numLastRow_Sell_Lock)).value
        sellLockedTargets = [target for target in sellLockedTargets if len(target) > 0]
        momentumTargets = [buyLockedTargets, sellLockedTargets]
        return momentumTargets
    
    def calculateForMomentumShares(self):
        momentumTargets = self.getMomentumTargets()
        sheetNames = ['两融网格', '简易网格', '目标市值', '目标市值两融', '普通网格']
        sht = self.wb.sheets['趋势份额']
        header = sht.range('AD1', 'AJ1').value
#        numLastRow_Region = sht.range('AD1').current_region.last_cell.row
#        for i in range(numLastRow_Region+1):
#            cellValue = sht.range('AD'+str(i+1)).value
#            if cellValue == None or cellValue == "None" or cellValue == "nan":
#                numLastRow = i
#                break
        momentumTradingTasks = []
        for sheetName in sheetNames:
            task = calculateForMomentumShare(self.wb, sheetName, header,momentumTargets)
            momentumTradingTasks.append(task)
        tasks = pd.concat(momentumTradingTasks)
        tasks_valided = tasks[tasks['操作价格'] != 0.0].copy().reset_index(drop=True)
#        for i in range(len(tasks_valided)):
#            task = list(tasks_valided.loc[i,:])
#            sht.range('AD'+str(numLastRow+1+i)).value = task
        if len(tasks_valided) > 0:
#            Helper.sendEmail('Need assistance in momentum', tasks_valided.to_string(), 'chenjiayi_344@hotmail.com')
            return -1, tasks_valided
        return 0, tasks_valided
        
    def updateGoldPrice(self):
        shf = self.wb.sheets['贵金属查询']
        tempUrl = 'http://www.icbc.com.cn/ICBCDynamicSite/Charts/GoldTendencyPicture.aspx'
        tempHTML = getHTML(tempUrl,timeLimit=40)
        tempContent = tempHTML.read()
        tempStr = tempContent.decode("UTF-8")
        tree = html.fromstring(tempStr) 
        tables = [ e for e in tree.iter() if e.tag == 'table']
        eps_table = tables[8]
        table_rows = [ e for e in eps_table.iter() if e.tag == 'tr']
#        results = [ e.text_content() for e in table_rows[0].iter() if e.tag == 'th']
        results = []
        for row in table_rows[1:]:
            cell_content = [ e.text_content() for e in row.iter() if e.tag == 'td']
            results.append(cell_content)
        results = [[row.replace('\r\n', '') for row in result] for result in results if len(result) > 0]
        results = [[row.replace(' ', '') for row in result] for result in results if len(result) > 0]
        shf.range('A2').value = results  
    
    def getLastTasks(self):
        targetDate = getLastTradedTime('log.txt')+relativedelta(days=1)
        print('getLastTasks buildTargetValueTasks targetDateStr {}'.format(dateTimeToDateStr(targetDate)))
        ordersTable = sheetToDF(self.wb.sheets['Preorders'])
        ordersTable = self.buildTargetValueTasks(ordersTable, dateTimeToDateStr(targetDate))
        ordersTable.loc[(ordersTable['Amount']==0) & (ordersTable['Platform']=='定期调平'), 'TradeCode' ] = 3;
        ordersTable = self.buildMomentumTasks(ordersTable)
        # momentum 28
        ordersTable_28  = ordersTable[ordersTable['Remark'] == '自动二八'].copy()
        ordersTable = ordersTable.drop(ordersTable[ordersTable['Remark'] == '自动二八'].index)
        ordersTable_28 = ordersTable_28.sort_values(by=['Amount'])
        ordersTable = pd.concat([ordersTable, ordersTable_28]).reset_index(drop=True)  
        # momentum industry
        ordersTable_industry  = ordersTable[ordersTable['Remark'] == '行业轮动'].copy()
        ordersTable = ordersTable.drop(ordersTable[ordersTable['Remark'] == '行业轮动'].index)
        ordersTable_industry = ordersTable_industry.sort_values(by=['Amount'])
        ordersTable = pd.concat([ordersTable, ordersTable_industry]).reset_index(drop=True)
        # momentum day K
        ordersTable_dayK  = ordersTable[ordersTable['Remark'] == '日K交易'].copy()
        ordersTable = ordersTable.drop(ordersTable[ordersTable['Remark'] == '日K交易'].index)
        ordersTable_dayK = ordersTable_dayK.sort_values(by=['Amount'])
        ordersTable = pd.concat([ordersTable, ordersTable_dayK]).reset_index(drop=True)
        
        ordersTable = self.removeInvalidTasks(ordersTable)
        columns = list(ordersTable.columns)
        lastColumnIdx = columns.index('Remark')
        selectedColumns = columns[:lastColumnIdx+1]
        df_lastTasks = ordersTable[selectedColumns]
        df_lastTasks = df_lastTasks[df_lastTasks['TradeCode'] > 0]
        df_lastTasks.loc[:, 'Code'] = df_lastTasks.loc[:, 'Code'].apply(numberToStr)
        return df_lastTasks.copy().reset_index(drop=True)
    
    def removeInvalidTasks(self, ordersTable):
        df_latestStockPrices = sheetToDF(self.wb.sheets['股票查询'])
        pipeline  = pdp.ApplyByCols('股票代码', getStockNumberStr, '股票代码', drop=False)
        df_latestStockPrices = pipeline(df_latestStockPrices)
        ordersTable['upLimit'] = 0
        ordersTable['downLimit'] = 0
        for i in range(len(ordersTable)):
            amount = ordersTable.loc[i, 'Amount']
            if amount != 0:
                stockCode   = numberToStr(ordersTable.loc[i, 'Code'])
                targetPrice = round(ordersTable.loc[i, 'Price'], 3)
                price_close = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == stockCode, '当前价'].values[0]
                upLimit   = round(price_close*1.1, 3)
                downLimit = round(price_close*0.9, 3)
                ordersTable.loc[i, 'upLimit'] = upLimit
                ordersTable.loc[i, 'downLimit'] = downLimit
                if targetPrice < downLimit or targetPrice > upLimit:
                    ordersTable.loc[i, 'TradeCode'] = -2
        return ordersTable
                    
    
    def compareWithLastTasks(self):
        df_lastTasks = self.getLastTasks()
        self.updateStockSheetLive()
        df_latestStockPrices = sheetToDF(self.wb.sheets['股票查询'])
        pipeline  = pdp.ApplyByCols('股票代码', getStockNumberStr, '股票代码', drop=False)
        df_latestStockPrices = pipeline(df_latestStockPrices)
        df_lastTasks['今开'] = 0
        df_lastTasks['今收'] = 0
        df_lastTasks['最高'] = 0 
        df_lastTasks['最低'] = 0
        df_lastTasks['成交价'] = 0
#        df_lastTasks['TradingPotential'] = False
        
        for i in range(len(df_lastTasks)):
            stockCode   = df_lastTasks.loc[i, 'Code']
            targetPrice = round(df_lastTasks.loc[i, 'Price'], 3)
            price_open  = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == stockCode, '今开'].values[0]
            price_close = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == stockCode, '当前价'].values[0]
            price_max   = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == stockCode, '最高'].values[0]
            price_min   = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == stockCode, '最低'].values[0]
            df_lastTasks.loc[i, '今开'] = price_open
            df_lastTasks.loc[i, '今收'] = price_close
            df_lastTasks.loc[i, '最高'] = price_max
            df_lastTasks.loc[i, '最低'] = price_min
            if df_lastTasks.loc[i, 'Amount'] > 0:
                if targetPrice >= price_min:
                    if targetPrice > price_open:
                        df_lastTasks.loc[i, '成交价'] = price_open
                    else:
                        df_lastTasks.loc[i, '成交价'] = targetPrice
            elif df_lastTasks.loc[i, 'Amount'] < 0:
                if targetPrice <= price_max:
                    if targetPrice < price_open:
                        df_lastTasks.loc[i, '成交价'] = price_open
                    else:
                        df_lastTasks.loc[i, '成交价'] = targetPrice
            else:
                if df_lastTasks.loc[i, 'Platform'] =='定期调平':
                    df_lastTasks.loc[i, '成交价'] = price_close
        df_lastTasks_traded = df_lastTasks[df_lastTasks['成交价'] != 0]
#        df_lastTasks_traded  = df_lastTasks
        hasTraded = len(df_lastTasks_traded) > 0
        return  hasTraded, df_lastTasks_traded
    
    def buildLockBuyTask(self, df, i, newOrdersTable, nonSupportedList):
        numRowsStart = len(newOrdersTable)
        name = df.loc[i, '基金名称']
        code = numberToStr(df.loc[i, '基金代码'])
        currentPrice = df.loc[i, '当前价格']
        amount = 0
        if not df.loc[i, '锁买'] == '锁买':
            value =  df.loc[i, '锁买金额']
            amount_lockBuy = df.loc[i, '锁买份额']
            if value > 0 or amount_lockBuy > 0:
                amount = round(value/currentPrice, -2)
                amount = max(amount, amount_lockBuy)
        if amount > 0:
            price = currentPrice*1.04 #make sure to buy
            priceDiff = price/currentPrice-1
            remark = '趋势买'
            if code in nonSupportedList:
                tradeCode = 1.0
            else:
                tradeCode = 3.0
        else:
            price = currentPrice
            tradeCode = -1
            priceDiff = 0
            remark = '无操作'        
        newOrdersTable.loc[numRowsStart, 'Name'] = name
        newOrdersTable.loc[numRowsStart, 'Code'] = code
        newOrdersTable.loc[numRowsStart, 'TradeCode'] = tradeCode
        newOrdersTable.loc[numRowsStart, 'Price'] = round(price,3)
        newOrdersTable.loc[numRowsStart, 'Amount'] = amount
        newOrdersTable.loc[numRowsStart, 'PriceDiff'] = priceDiff
        newOrdersTable.loc[numRowsStart, 'Remark'] = remark
        newOrdersTable.loc[numRowsStart, 'Platform'] = '华泰'
        return newOrdersTable
        
    def buildLockSellTask(self, df, i, newOrdersTable):
        numRowsStart = len(newOrdersTable)
        name = df.loc[i, '基金名称']
        code = numberToStr(df.loc[i, '基金代码'])
        currentPrice = df.loc[i, '当前价格']
        amount = 0
        if not (df.loc[i, '锁卖'] == '锁卖'):
                amount = df.loc[i, '锁卖份额']
        if amount < 0:
            tradeCode = 2.0
            costPrice  = round(df.loc[i, '锁卖成本'], 3)
            price = max(currentPrice*0.96, costPrice) #make sure to sell but with fail price
            priceDiff = price/currentPrice-1
            remark = '趋势卖'
        else:
            price = currentPrice
            tradeCode = -1
            priceDiff = 0
            remark = '无操作'
        newOrdersTable.loc[numRowsStart, 'Name'] = name
        newOrdersTable.loc[numRowsStart, 'Code'] = code
        newOrdersTable.loc[numRowsStart, 'TradeCode'] = tradeCode
        newOrdersTable.loc[numRowsStart, 'Price'] = round(price,3)
        newOrdersTable.loc[numRowsStart, 'Amount'] = amount
        newOrdersTable.loc[numRowsStart, 'PriceDiff'] = priceDiff
        newOrdersTable.loc[numRowsStart, 'Remark'] = remark
        newOrdersTable.loc[numRowsStart, 'Platform'] = '华泰'
        return newOrdersTable
    
    def buildMomentumTasks(self, ordersTable):
        orderTableHeaders = list(ordersTable.columns)
        nonSupportedList = list(ordersTable['NonSupported'])
        nonSupportedList = [numberToStr(e) for e in nonSupportedList if isnumeric(e)]
        newOrdersTable = pd.DataFrame(columns=orderTableHeaders)
        sht = self.wb.sheets['趋势份额']
        header = sht.range('A1', 'Y1').value
        numLastRow_Region = sht.range('A1').current_region.last_cell.row
        for i in range(numLastRow_Region+1):
            cellValue = sht.range('A'+str(i+1)).value
            if cellValue == None or cellValue == "None" or cellValue == "nan":
                numLastRow = i
                break
        data =  sht.range('A2', 'Y'+str(numLastRow)).value
        df = pd.DataFrame(data=data, columns=header)

        if len(df) > 0:
            for i in range(len(df)):
                newOrdersTable = self.buildLockBuyTask(df, i, newOrdersTable, nonSupportedList)
                newOrdersTable = self.buildLockSellTask(df, i, newOrdersTable)
        if len(newOrdersTable) > 0:
            ordersTable = pd.concat([ordersTable, newOrdersTable]).reset_index(drop=True)
        return ordersTable
    
    def buildTargetValueTasks(self, ordersTable, targetDateStr):
        print('Target date {}, today {}, tomorrow {}'.format(targetDateStr, getTodayDateStr(), getTomorrowDateStr()))
        orderTableHeaders = list(ordersTable.columns)
        nonSupportedList = list(ordersTable['NonSupported'])
        nonSupportedList = [numberToStr(e) for e in nonSupportedList if (e is not None and e != 'None')]
        targetValueSheetNames = ['目标市值两融', '目标市值']
        newOrdersTable = pd.DataFrame(columns=orderTableHeaders)
        targetTasks = []
        for sheetname in targetValueSheetNames:
            sht = self.wb.sheets[sheetname]
            df  = sheetToDF(sht)
            df.loc[:, '下期时间'] = df.loc[:, '下期时间'].apply(numberToDateStr)
            df2 = df[df['下期时间']==targetDateStr].copy().reset_index(drop=True)
            numRowsStart = len(newOrdersTable)
            if len(df2) > 0:
                for i in range(len(df2)):
                    name = df2.loc[i, '基金名称']
                    code = numberToStr(df2.loc[i, '基金代码'])
                    price = df2.loc[i, '当前价格']
                    amount = df2.loc[i, '调整份数']
                    if amount < 0:
                        tradeCode = 2.0
                        price *= 0.96 #make sure to sell
                        priceDiff = -0.04
                    elif amount == 0:
                        tradeCode = -1
                        priceDiff = 0
                    else:
                        price *= 1.04 #make sure to buy
                        priceDiff = 0.04
                        if code in nonSupportedList:
                            tradeCode = 1.0
                        else:
                            tradeCode = 3.0
                    newOrdersTable.loc[numRowsStart, 'Name'] = name
                    newOrdersTable.loc[numRowsStart, 'Code'] = code
                    newOrdersTable.loc[numRowsStart, 'TradeCode'] = tradeCode
                    newOrdersTable.loc[numRowsStart, 'Price'] = price
                    newOrdersTable.loc[numRowsStart, 'Amount'] = amount
                    newOrdersTable.loc[numRowsStart, 'PriceDiff'] = priceDiff
                    newOrdersTable.loc[numRowsStart, 'Remark'] = sheetname
                    newOrdersTable.loc[numRowsStart, 'Platform'] = '定期调平'
                    numRowsStart += 1
                    targetTasks.append([name, sheetname])
        if len(newOrdersTable) > 0:
            ordersTable = pd.concat([ordersTable, newOrdersTable]).reset_index(drop=True)
            for jj in range(len(newOrdersTable)):
                name, sheetname = targetTasks[jj]
                ordersTable.loc[(ordersTable['Name'] ==  name) & \
                                (ordersTable['Remark'] ==  sheetname)\
                                & (ordersTable['Platform']  != '定期调平'),\
                                'TradeCode'] = -2
        return ordersTable
             
    
    # def updateFor28Trade(self):
    #     targets = ['S000016', 'S399300', 'S000905', 'S399006']
    #     targetObjs = [self.objMap[t] for t in targets]
    #     self.updateSheetsV2(targetObjs)
    #     sht = targetObjs[0].sht
    #     numLastRow = sht.range('A1').current_region.last_cell.row
    #     lastUpdateDateStr = numberToStr(sht.range(numLastRow, 1).value)
    #     return lastUpdateDateStr
    
    def dfFor28Trade(self, nDays):
        targets = ['S000016', 'S399300', 'S000905', 'S399006']
        header  = ['Date', 'SZ50', 'HS300', 'ZZ500','CYB']
        latestDates = []
        subobj_main = self.objMap['S000016']
        df_main = self.sql.getLastRows('S000016', nDays)
        for t in targets:
            subobj = self.objMap[t]
            latestDates.append(subobj.lastUpdatedDate)
        numDiffDays = [(t - latestDates[0]).days for t in latestDates]
        sameDate = [d == 0 for d in numDiffDays]
        if all(sameDate):
            date = df_main[subobj_main.DateStr]
            date = [numberToDateStr(d) for d in date]
            data = []
            for t in targets:
                df_temp        = self.sql.getLastRows(t, nDays)
                data_temp = list(df_temp[subobj_main.TCloseStr])
                data.append(data_temp)
            data = [date] + data
            return pd.DataFrame(zip(*data), columns = header)
        else:
            raise Exception("Latest dates for 28 trading are not the same")                 
    

    
    def momentumDayKTrade(self, times=10):
        sht = self.wb.sheets['日线交易']
        df = self.sql.getLastRows('S000985', 3*24)
        df = BBI(df)
        df = MAs(df, [5, 10 ,12, 20])
        df = AMAs(df)
        
        latestDateStr = numberToDateStr(float(df['日期'].iloc[-1]))
        df_record = getTargetArea(sht, 'AG', 'AL')
        df_task   = getTargetArea(sht, 'A', 'C')
        startCol1 = 'AB'
        startCol2 = 'AC'
        startCol3 = 'AD'
        startRow = 28
        task_names = df_task['策略编号'].unique()
        for i in range(len(task_names)):
            name = task_names[i]
            df2 = df_record[df_record['策略名称'] == name]
            if  len(df2) > 0:
                lastTradedDate_temp = dateTimeToDateStr(df2['操作日期'].iloc[-1])
            else:
                lastTradedDate_temp = getDeltaDateStr(-40)
            diffDay_temp = getDaysBetweenDateStrs(latestDateStr, lastTradedDate_temp)
            daysInName = name.split('-')
            short_str = daysInName[0]
            long_str  = daysInName[1]
            requiredDay_temp = int(daysInName[2].split('天')[0])
            if diffDay_temp >= requiredDay_temp:
                first_value  = df[short_str].iloc[-1]
                second_value = df[long_str].iloc[-1]
                sht.range(startCol1+str(startRow+i)).value = first_value
                sht.range(startCol2+str(startRow+i)).value = second_value
                sht.range(startCol3+str(startRow+i)).value = time.strftime("%d/%m/%Y, %H:%M:%S")
        


    def momentum28Trade(self):
        targets = ['SZ50', 'HS300', 'ZZ500','CYB']
        numTarget = len(targets)
        latestDateStr = dateTimeToDateStr(self.objMap['S000016'].lastUpdatedDate)
        sht = self.wb.sheets['自动二八']
        df_record = getTargetArea(sht, 'AH', 'AM')
        df_task   = getTargetArea(sht, 'A', 'C')
        diffDays     = []
        checkingDays =  []
        requiredDays = []
        task_names = df_task['策略编号'].unique()
        for i in range(len(task_names)):
            name = task_names[i]
            df2 = df_record[df_record['策略名称'] == name]
            if  len(df2) > 0:
                lastTradedDate_temp = dateTimeToDateStr(df2['操作日期'].iloc[-1])
            else:
                lastTradedDate_temp = getDeltaDateStr(-40)
            diffDay_temp = getDaysBetweenDateStrs(latestDateStr, lastTradedDate_temp)
            daysInName = name.split('-')
            checkingDay_temp = int(daysInName[0].split('天')[0])
            requiredDay_temp = int(daysInName[1].split('天')[0])
            diffDays.append(diffDay_temp)
            checkingDays.append(checkingDay_temp)
            requiredDays.append(requiredDay_temp)
        
        df_hist = self.dfFor28Trade(max(diffDays)+30)
        for i in range(len(task_names)):
            diffDay = diffDays[i]
            requiredDay = requiredDays[i]
            checkingDay = checkingDays[i]
            if diffDay > requiredDay:#minimum day offsets
                codes = [0, 0, 0, 0]
                percents = [0, 0, 0, 0]
                for j in range(numTarget):#get performance for each target
                    percents[j] = df_hist[targets[j]].iloc[-1]/df_hist[targets[j]].iloc[-1*checkingDay-1] - 1
                if any([p>0 for p in percents]): #if any target is increasing
                    maxIdx = percents.index(max(percents))
                    codes[maxIdx] = 1 #find the maximum increased target
                for j in range(numTarget):
                    sht.range('P'+str(i*numTarget+j+2)).value = codes[j]
                    sht.range('Q'+str(i*numTarget+j+2)).value = percents[j]
    
    def momentumIndustry(self):
        offSetDays = 12
        sht = self.wb.sheets['行业轮动']
        targetDate = sht.range('AB13').value
        if getTodayDate() > targetDate:
            shtList = getTargetArea(sht, 'B').values
            sheetNames = [s[0] for s in shtList]
            percents = []
            # targetObjs = [ self.objMap[sheetName] for sheetName in sheetNames]
            for sheetName in sheetNames:
                df_temp = self.sql.getLastRows(sheetName, offSetDays)
                if '日期' in  df_temp.columns:
                    temp_TCloseStr = '收盘价'
                else:
                    temp_TCloseStr = '累计净值'
                temp_LatestTClose = df_temp.loc[len(df_temp)-1, temp_TCloseStr]
                temp_offsetTClose = df_temp.loc[0, temp_TCloseStr]
                percents.append([temp_LatestTClose/temp_offsetTClose-1])
            sht.range('C2').value = percents    
    
    def reminder(self):
        sht = self.wb.sheets['平台账本']
        if sht.range('G33').value > 0.04:
            self.msg.append('目标市值场外版临时操作')
#            Helper.sendEmail('临时操作', '目标市值场外版临时操作', 'chenjiayi_344@hotmail.com')
        if sht.range('G34').value > 0.04:
            self.msg.append('目标市值均值回归临时操作')
#            Helper.sendEmail('临时操作', '目标市值均值回归临时操作', 'chenjiayi_344@hotmail.com')
        tomorrowDate = getTodayDate()+timedelta(days=1)
        sht1 = self.wb.sheets['目标市值场外版']
        sht2 = self.wb.sheets['目标市值均值回归']
        date1_list = getTargetArea(sht1, 'U').values
        date2_list = getTargetArea(sht2, 'V').values
        date1_list = [datetime.utcfromtimestamp(np.datetime64(date[0], 's').astype(int)) for date in date1_list]
        date2_list = [datetime.utcfromtimestamp(np.datetime64(date[0], 's').astype(int)) for date in date2_list]
        if tomorrowDate in date1_list:
            self.msg.append('目标市值场外版定时操作')
#            Helper.sendEmail('定时操作', '目标市值场外版定时操作', 'chenjiayi_344@hotmail.com')
        if tomorrowDate in date2_list:
            self.msg.append('目标市值均值回归定时操作')
#            Helper.sendEmail('定时操作', '目标市值均值回归定时操作', 'chenjiayi_344@hotmail.com')
        sht3 = self.wb.sheets['备忘录']    
        df3  = getTargetArea(sht3, 'A', 'B')
        df3_task = df3[df3['日期'] == tomorrowDate].copy()
        if len(df3_task) > 0:
            self.msg.append('备忘录:')
            self.msg.append(df3_task.to_string())
#            Helper.sendEmail('备忘录', df3_task.to_string(), 'chenjiayi_344@hotmail.com')
    
    def getOrderedTasks(self, ordersTable):
        return ordersTable[ordersTable['TradeCode'] > 0].copy().reset_index(drop=True)
    
    def writeOrderedTasks(self, orderedTable):
#        orderedTable = ordersTable[ordersTable['TradeCode'] > 0].copy().reset_index(drop=True)
        if len(orderedTable) > 0 :
            targetCols = orderedTable.columns[:9].to_list()
            orderedTable = orderedTable.loc[:, targetCols]
            sht = self.wb.sheets['Ordered']
            sht.clear_contents()
            sht.range('A1').options(index=False).value = orderedTable
            sht.range('K1').value = '下单时间'
            sht.range('L1').value = getTodayDate()
            sht.range('M1').value = time.strftime("%H:%M:%S")
            
    def getTotalValue(self):
        sht = self.wb.sheets['平台账本']
        summary_value = round(sht.range('O27').value, 2)
        msg1 = 'Summary: {}'.format(summary_value)
        if msg1 not in self.msg:
            self.msg.insert(0, msg1)
    
    def sendSummary(self):
        if len(self.imgs) > 0:
            imgPath = os.path.join(os.getcwd(), 'Task'+getTodayDateStr()+'.png')
            img = mergeImg(self.imgs)
            img.save(imgPath)
            self.imgPath = imgPath
        msg = ''
        for m in self.msg:
            msg = msg + m + '\n'
        if len(self.imgPath) > 0:
            sendEmail('今日汇总 From Simulation', msg, 'chenjiayi_344@hotmail.com', self.imgPath)
        else:
            sendEmail('今日汇总 From Simulation', msg, 'chenjiayi_344@hotmail.com')
    
    def calculate(self):
        self.wb.app.calculate()
    
    def save(self):
        self.wb.save()
    
    def close(self):
        self.wb.app.kill()

def printTable(table):
    msg = tb.tabulate(table.values, table.columns, tablefmt="pipe")
    print(msg)
    return msg

def summaryTraded(sysObj, df_lastTasks_traded):

    if len(df_lastTasks_traded) > 0:
        targetCols = df_lastTasks_traded.columns[[0,1,3,4,5,7,8, -1]]
        df_lastTasks_traded = df_lastTasks_traded.loc[:, targetCols]
        df_lastTasks_traded['Price'] = df_lastTasks_traded['Price'].apply(lambda x: round(x, 3))
        df_lastTasks_traded['PriceDiff'] = df_lastTasks_traded['PriceDiff'].apply(lambda x: '{}%'.format(round(x*100, 2)))
        print('These traded tasks are written:')
        printTable(df_lastTasks_traded)
        # sysObj.imgs.append(dfToImg(df_lastTasks_traded))%TODO
#        sysObj.msg.append('Traded task:')
#        sysObj.msg.append(msg2)
#    Helper.sendEmail('今日汇总', msg1+'\n'+msg2 , 'chenjiayi_344@hotmail.com')

def callBatchMethod(sysObj, methodStr):
    loopGuard = 3
    returnCode = 1
    while loopGuard > 0:
        try:
            sysObj.batchMethods[methodStr].__call__()
            print("Updating {} done\n".format(methodStr))
            returnCode = 0
            break
        except:
            loopGuard -= 1
            print("Something is wrong during calling {}, try {} times\n".format(methodStr, loopGuard))
            returnCode = -1
    return returnCode
 
'''
    subprocess.run(["D:\\Dropbox\\For daily life\\Investment\\RunHuatai.exe"])    
    from SmartQ_Python import SmartQ_Python
    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
    SmartQ_Python(ordersTable)
    
'''

def updatingOnly(sysObj, weekday=1, afterEarlySummary=False):
    exitCode = 0
    t = time.time()
    try:
#        sysObj = Trade('本金账本.xlsx')
        sysObj.batchMethods = {}
        sysObj.batchMethods['updateMomentums'] = sysObj.updateMomentums
        sysObj.batchMethods['updateRelativeMomentums'] = sysObj.updateRelativeMomentums
        sysObj.batchMethods['calculateForMomentumShares'] = sysObj.calculateForMomentumShares
        sysObj.batchMethods['updateStockSheetLive'] = sysObj.updateStockSheetLive
        sysObj.batchMethods['updateFundSheetLive'] = sysObj.updateFundSheetLive
        sysObj.batchMethods['updateGoldPrice'] = sysObj.updateGoldPrice
        print("Read xlsx file done\n")
        isTradingTime = sysObj.atTradingTime()
        if not isTradingTime:
## Part 1:
            print("Updating not at trading time\n")
            nowTimeStru        = datetime.now().timetuple()
            if nowTimeStru.tm_wday != 6 and not afterEarlySummary:
                hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
            else:
                hasTraded = False
#            hasTraded = False 
            if hasTraded: 
                writeTradedTasks(sysObj, df_lastTasks_traded)
            callBatchMethod(sysObj, 'updateFundSheetLive')
            sysObj.calculate()
            print("Calculating done 0\n")
            sysObj.getTotalValue()
            print("GetTotalValue done\n")
            if hasTraded:  #have to update first to calculate the summary     
                summaryTraded(sysObj, df_lastTasks_traded)
## Part 2:
            sysObj.initialSubjects()
            print("Initializing subjects done\n")
            # callBatchMethod(sysObj, 'updateSheetsV2')
            sysObj.momentum28Trade()
            print("Updated for momentum 28 trade\n")
            sysObj.momentumIndustry()
            print("Updated for momentum industry\n")
            if weekday != 5:
                sysObj.momentumDayKTrade()
                print("Updated for momentum day K trade\n")
            if sysObj.atWeekend():
                print("Have to update momentum at weekends\n")
                callBatchMethod(sysObj, 'updateMomentums')
                callBatchMethod(sysObj, 'updateRelativeMomentums')
            else:
                print("Do not update momentum at weekdays\n")
            exitCode, tasks_valided = sysObj.calculateForMomentumShares() #write momentum shares
            if exitCode == -1: #move momentum share to sheets['趋势份额']
                writeMomentTasks(sysObj, tasks_valided)
                sysObj.calculate()
                print("Calculating done 1\n")
            callBatchMethod(sysObj, 'updateGoldPrice')
            sysObj.reminder()
            sysObj.calculate()
            print("Calculating done 2\n")
#            sysObj.save()
#            print("Saving done 2\n")
        else:
            print("During trading time\n")
            callBatchMethod(sysObj, 'updateStockSheetLive')
            sysObj.calculate()
            print("Calculating done 3\n")
#            sysObj.save()
#            print("Saving done 3\n")
        t_usage = time.time() - t
        finishingMessage = "All tasks are finished in {:.2f} seconds".format(t_usage)
        print(finishingMessage)       
        #autopy.alert.alert(finishingMessage, "Trading System")     
    except SettingWithCopyError:
        print('handling..')
        frameinfo = getframeinfo(currentframe())
        print(frameinfo.lineno)
    return exitCode


def orderingOnly(sysObj):
    exitCode = 0
    sysObj.initialSubjects()
    targetDate = getTomorrowDate()
    print('OrderingOnly buildTargetValueTasks targetDateStr {}'.format(dateTimeToDateStr(targetDate)))
    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
    ordersTable = sysObj.buildTargetValueTasks(ordersTable,dateTimeToDateStr(targetDate))
    ordersTable = sysObj.buildMomentumTasks(ordersTable)
    ordersTable = sysObj.removeInvalidTasks(ordersTable)
    orderedTable = sysObj.getOrderedTasks(ordersTable)
    targetCols   = orderedTable.columns[[0,1,3,4,5,7,8 ]]
    orderedTable_new = orderedTable.loc[:, targetCols]
    orderedTable_new['Price'] = orderedTable_new['Price'].apply(lambda x: round(x, 3))
    orderedTable_new['PriceDiff'] = orderedTable_new['PriceDiff'].apply(lambda x: '{}%'.format(round(x*100, 2)))
    print('These tasks are prapared for order:')
    printTable(orderedTable_new)

    requiredCredit = 0
    requiredCash   = 0
    sht = sysObj.wb.sheets['平台账本']
    availableCredit = round(sht.range('Q26').value, 2)
    availableCash   = round(sht.range('Q27').value, 2)
    for i in range(len(orderedTable)):
        if orderedTable.loc[i, 'TradeCode'] == 3:
            requiredCredit += round(orderedTable.loc[i, 'Amount']*orderedTable.loc[i, 'Price'], 2)
        elif orderedTable.loc[i, 'TradeCode'] == 1:
            requiredCash += round(orderedTable.loc[i, 'Amount']*orderedTable.loc[i, 'Price'], 2)
    if requiredCredit + requiredCash > availableCredit + availableCash:
        msg  = "Required credit {} and cash {}, but available credit {} and cash {}"\
        .format(requiredCredit, requiredCash, availableCredit, availableCash)
        sendEmail('Alert', msg, 'chenjiayi_344@hotmail.com')
        return -1
    else:
        if len(orderedTable) > 0:
            # exitCode, sysObj = SmartQ_Python(sysObj, ordersTable, availableCredit)%TODO
            if exitCode == 0:
                sysObj.writeOrderedTasks(orderedTable)
                sysObj.getTotalValue()
                sysObj.sendSummary()
                sysObj.save()
                sysObj.close() #Autoclose only when confirmaton is implemented
                print("Closing done 2\n")
            else:
                print("Error in SmartQ\n")
    return exitCode, sysObj
            

def runRoutine(weekday=1, afterEarlySummary=False):
    exitCode = 0
    db_path = 'Resources.db'
    sql = Database(db_path)
    sysObj = Trade('本金账本.xlsx', sql)
    updatingOnly(sysObj, weekday, afterEarlySummary) 
    sleep(5)
    if weekday != 5:
        print("Orerding now")
        sysObj.save()
        print("Saving done 11\n")
        sysObj.close()
        exitCode = 0
        pass
        # subprocess.run(["D:\\Dropbox\\For daily life\\Investment\\RunHuatai.exe"])  
        # exitCode, sysObj = orderingOnly(sysObj)
    else:
        exitCode = 0
        sysObj.save()
        print("Saving done 00\n")
        sysObj.close()
    sysObj.sendSummary()
    return exitCode

if __name__ == '__main__':
#    exitCode = runRoutine()
    db_path = 'Resources.db'
    sql = Database(db_path)
    # sql.createDB(xlsx_path, db_path)
    print(sql.getLastRows('S000985', 10))
    sleep(5)
    sql.start()
    sleep(5)
    sysObj = Trade('本金账本.xlsx',sql)
    updatingOnly(sysObj, weekday=1)
    sysObj.sendSummary()


'''
import pickle
import os
resultFile = os.path.join(os.getcwd(), 'df_lastTasks_traded.out')
if os.path.isfile(resultFile):
    os.remove(resultFile)
f = open(resultFile, 'wb')
pickle.dump(tasks_valided, f)
f.close()

import pickle
import os
resultFile = os.path.join(os.getcwd(), 'df_lastTasks_traded.out')
f = open(resultFile, 'rb')
tasks_valided = pickle.load(f)
f.close()


'''


'''
 sht = sysObj.wb.sheets['趋势份额']
 formula = sht.range('AK36').formula
 newFormula = formula.replace('36', '37')
 sht.range('AK37').formula = newFormula
 
 formulas = sht.range('AK36', 'AP36').formula
 type(formulas)
formulas = formulas[0]
formulas = list(formulas)
newFormulas = [s.replace('36', '37') for s in formulas]
newFormulas = tuple(newFormulas)
sht.range('AK37', 'AP37').formula = newFormulas
sht.range('AL37').formula_array = newFormulas[1]

'''
