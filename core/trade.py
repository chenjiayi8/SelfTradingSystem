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


logs:
    25/01/2022: Trade and HuataiPlatform check credit and cash to confirm and make orders

"""
#import modin.pandas as pd
#import sys
#sys.path.append('D:\\LinuxWorkFolder\\TUD\\Python\\Library')
import pywinauto
import os
# from addLibraries import Helper
import xlwings as xw
from lxml import html
import time
from datetime import datetime
from datetime import timedelta
# import subprocess
#import pytz
#from dateutil.relativedelta import relativedelta
# import multiprocessing
# from multiprocessing.dummy import Pool as ThreadPool
# import re
# import math
# import itertools
import pandas as pd
import pdpipe as pdp
#import autopy
# import subprocess
# from socket import timeout
# import numpy as np
#from addLibraries import Helper
from inspect import currentframe, getframeinfo
from pandas.core.common import SettingWithCopyError
pd.options.mode.chained_assignment = 'raise'
from dateutil.relativedelta import relativedelta
# import ExcelHelper
import tabulate as tb
# from SmartQ_PythonV2 import SmartQ_Python, dfToImg
# from SelfTradingSystem.io.subject import Subject, updateRelativeMomentumWrapper
from SelfTradingSystem.util.stock import (
    getStock, getHTML, getFundLatest, getStocksBatchFromTencent,
    BBI, MAs, AMAs, getStocksBatch, getFund, getFundLatestBatchFromTencent
    )
from SelfTradingSystem.util.others import (
    getLastTradedTime, isSameDate,
    isnumeric, mergeImg, sleep
    )
from SelfTradingSystem.io.excel import (
    sheetToDF, indCell, getTargetArea, removeMargin, dfToDatabaseDF,
    updateTradedTasks, writeMomentTasks, getColumnStr,
    )     

from SelfTradingSystem.util.convert import (
    numberToStr, numberToDateStr, getTodayDate, numberToDateTime,
    getStockNumberStr, dateTimeToDateStr, getTomorrowDateStr,
    getTodayDateStr, getDeltaDateStr, getDaysBetweenDateStrs,
    getTomorrowDate, dateStrToDateTime, convertShtToDB,
    )
from SelfTradingSystem.core.strategy import (
    calculateForMomentumShare,
)
from SelfTradingSystem.util.remindMe import sendEmailBatch as sendEmail
from SelfTradingSystem.io.database import Database
from SelfTradingSystem.core.operation import dfToImg, strToImg
# from SelfTradingSystem.util.stock import getFundLatest

class OrderDatabase(Database):
    def __init__(self, db):
        self.db=db
        self.setSubjectNames()
        import multiprocessing as mp
        self.writing = mp.Value('i', 0)
        
    def getValue(self, sheetName, colStr, rowNum):
        value = ''
        with self.create_connection_for_read() as conn:
            sqlStr = 'SELECT {} from {} where rowid = {}'.\
            format(colStr, sheetName,  rowNum)
            cur = conn.cursor()
            cur.execute(sqlStr)
            value = cur.fetchall()
            return value[0][0]
        return value
    
        
class Trade:
    def __init__(self, xlsxName, sql=[],margin_buying_disabled=False):
        self.xlsxName = xlsxName
        self.sql = sql
        self.pywinauto_app = pywinauto.application.Application
        self.db_sql  = OrderDatabase('本金账本.db')
        self.WBInitialised = False
        # self.pool = ThreadPool(4)
        self.totalValueFromImg = 0
        self.totalValueFromSht = 0
        self.successfulTrading = -1
        self.credit_account = {}
        self.msg = []
        self.imgDict = {}
        self.imgPath = ''
        self.margin_buying_disabled=margin_buying_disabled
        self.logFile = os.path.join(os.getcwd(), getTodayDateStr()+'.txt')
        self.create_log_file()
 
    def create_log_file(self):
        if not os.path.isfile(self.logFile):
            with open(self.logFile, 'a'):  # touch file
                os.utime(self.logFile, None)
                
    def write_log(self, msg):
        print(msg)
        try:
            if os.path.isfile(self.logFile):
                with open(self.logFile, 'a+') as f:
                    f.write('{}\n'.format(msg))
        except Exception:
            pass    
 
    def initialiseWB(self):
        if len(self.xlsxName) > 0 and not self.WBInitialised:
            self.wb = xw.Book(self.xlsxName)
            self.write_log("Load xlsx file done")
            self.sheetNames = [self.wb.sheets[s].name for s in range(self.wb.sheets.count)]
            self.shf_config = self.wb.sheets['Menu']
            self.s_Str = 'B'
            self.f_str = 'F'
            self.row_num = 12
            self.numRowLastStock = self.shf_config.range(indCell(self.s_Str, 1)).current_region.last_cell.row
            self.numRowLastFund = self.shf_config.range(indCell(self.f_str, 1)).current_region.last_cell.row
            self.momentumMaps = []
            # self.momentumNameMaps = []
            for r in range(12,self.numRowLastStock+1):
                if self.shf_config.range(indCell(self.s_Str, r)).value != None:
                    self.momentumMaps.append(chr(ord(self.s_Str)+2)+str(r))
                    # self.momentumNameMaps.append(self.shf_config.range(indCell(chr(ord(self.s_Str)+1), r)).value)
            for r in range(12,self.numRowLastFund+1):
                if self.shf_config.range(indCell(self.f_str, r)).value != None:
                    self.momentumMaps.append(chr(ord(self.f_str)+2)+str(r))
                    # self.momentumNameMaps.append(self.shf_config.range(indCell(chr(ord(self.f_str)+1), r)).value)
            self.stockNumberStrs = [numberToStr(self.shf_config.range(indCell(self.s_Str, r)).value) for r in range(12, self.numRowLastStock+1) if self.shf_config.range(indCell(self.s_Str, r)).value != None ]
            self.fundNumberStrs = [numberToStr(self.shf_config.range(indCell(self.f_str, r)).value) for r in range(12, self.numRowLastFund+1) if self.shf_config.range(indCell(self.f_str, r)).value != None ]
            self.WBInitialised = True

    def initialSubjects(self):
        self.initialiseWB()
        targetSubjectNames = [ 'S' + subname for  subname in self.stockNumberStrs] + \
                 [ 'F' + subname for  subname in self.fundNumberStrs]
        self.tradeObjs = [ self.sql.objMap[subname] for subname in targetSubjectNames]
        self.objMap = dict(zip(targetSubjectNames, self.tradeObjs))
            
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
        df = self.sql.getDF('Menu')
        df.index = df['代码']
        for i in range(len(self.tradeObjs)):
            subjectname = self.tradeObjs[i].subjectname
            cellIdx = self.momentumMaps[i]
            momentum_result = df.loc[subjectname, '趋势']
            if len(momentum_result) > 0:
                self.shf_config.range(cellIdx).value = momentum_result
            
    def updateRelativeMomentums(self):
        df = self.sql.getDF('Menu')
        df.index = df['代码']
        columns = ['N天涨幅','T','T-1','T-2','T-3','T-4','T-5','T-6','T-7']
        shf = self.wb.sheets['趋势']
        for i in range(len(self.tradeObjs)):
            subjectname = self.tradeObjs[i].subjectname
            momentum_result = list(df.loc[subjectname, columns])
            shf.range('V'+str(i+2)).value = momentum_result 
            
    def updateStockSheetLive(self):
        sht_stockInfo = self.wb.sheets['股票查询']
        numRowsStock = sht_stockInfo.range('A1').current_region.last_cell.row
        tasks = sht_stockInfo.range('A2', 'A'+str(numRowsStock)).value
        tasks = [task for task in tasks if task is not None]
        numRowsStock = len(tasks)+1
        results = getStocksBatch(tasks)
        sht_stockInfo.range('C2',  'C'+str(numRowsStock)).value = results
        updatedTime =  [[time.strftime("%d/%m/%Y, %H:%M:%S")]]*(numRowsStock-1)
        sht_stockInfo.range('AJ2').value = updatedTime      
            
    def updateStockSheetLiveFromTencent(self):
        sht_stockInfo = self.wb.sheets['股票查询']
        numRowsStock = sht_stockInfo.range('A1').current_region.last_cell.row
        tasks = sht_stockInfo.range('A2', 'A'+str(numRowsStock)).value
        tasks = [task for task in tasks if task is not None]
        numRowsStock = len(tasks)+1
        results = getStocksBatchFromTencent(tasks)
        sht_stockInfo.range('C2',  'C'+str(numRowsStock)).value = results
        updatedTime =  [[time.strftime("%d/%m/%Y, %H:%M:%S")]]*(numRowsStock-1)
        sht_stockInfo.range('AJ2').value = updatedTime      
        
    def updateFundSheetLive(self):
        sht_fundInfo = self.wb.sheets['净值查询']
        numRows = sht_fundInfo.range('A1').current_region.last_cell.row
        endRowStr = 'A'+str(numRows)
        tasks = sht_fundInfo.range('A2', endRowStr).value
        tasks = [numberToStr(task) for task in tasks]
        results = getFundLatest(tasks)
        for i in range(2, numRows+1):
            result = results[i-2]
            if tasks[i-2] in ['162411']:
                result = getFund(tasks[i-2])[0]
            if len(result) > 3:
                sht_fundInfo.range('C'+str(i)).value = result
                sht_fundInfo.range('J'+str(i)).value = time.strftime("%d/%m/%Y, %H:%M:%S")
                
    def updateFundSheetLiveFromTencent(self):
        sht_fundInfo = self.wb.sheets['净值查询']
        numRows = sht_fundInfo.range('A1').current_region.last_cell.row
        endRowStr = 'A'+str(numRows)
        tasks = sht_fundInfo.range('A2', endRowStr).value
        tasks = [numberToStr(task) for task in tasks]
        results = getFundLatestBatchFromTencent(tasks)
        for i in range(2, numRows+1):
            result = results[i-2]
            if len(result) > 3:
                sht_fundInfo.range('C'+str(i)).value = result
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
        # sht = self.wb.sheets['Ordered']
        # header = sht.range('A1', 'I1').value
        # numLastRow_Region = sht.range('A1').current_region.last_cell.row
        # if numLastRow_Region == 1:
        #     return pd.DataFrame(columns=header)
        # for i in range(numLastRow_Region+1):
        #     cellValue = sht.range('A'+str(i+1)).value
        #     if cellValue == None or cellValue == "None" or cellValue == "nan":
        #         numLastRow = i
        #         break
        # data =  sht.range('A2', 'I'+str(numLastRow)).value
        # if numLastRow == 2:
        #     data = [data]
        df = self.db_sql.getDF('Ordered')
        df2 = df.loc[:, :'I'].copy()
        header = df2.loc[0, :].tolist()
        data = df2.loc[1:, :].values.tolist()
        df3 = pd.DataFrame(data=data, columns=header)
        removed_rows = []
        for i in range(len(df3)):
            cellValue = df3.loc[i, 'Name']
            if cellValue == '' or cellValue == None or cellValue == "None" or cellValue == "nan":
                removed_rows.append(i)
        
        df4 = df3.drop(df3.index[removed_rows])
        # df = pd.DataFrame(data=data, columns=header)
        targetDate = getLastTradedTime('log.txt')+relativedelta(days=1) 
        if not isSameDate(targetDate, getTodayDate()):
            return pd.DataFrame(columns=header)
        else:
            return df4.copy().reset_index(drop=True)

    
    def removeInvalidTasks(self, ordersTable):
        df_latestStockPrices = sheetToDF(self.wb.sheets['股票查询'])
        pipeline  = pdp.ApplyByCols('股票代码', getStockNumberStr, '股票代码', drop=False)
        df_latestStockPrices = pipeline(df_latestStockPrices)
        ordersTable['upLimit'] = 0
        ordersTable['downLimit'] = 0
        if not 'credit' in self.credit_account:
            maxCredit = 1e7
            maxCash = 1e7
        else:
            maxCredit = self.credit_account['credit']*0.99
            maxCash = self.credit_account['credit_cash']*0.99
        
        ordersTable = ordersTable.sort_values(by=['TradeCode',  'Amount'], ascending=False).copy().reset_index(drop=True)
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
                if self.margin_buying_disabled:#only selling
                    if not any([item in ordersTable.loc[i, 'Name'] for item in ['消费', '创成长', '中概']]): #buy 消费 etc.
                        if ordersTable.loc[i, 'TradeCode'] in [3, 3.0]: #disable margin buying
                            ordersTable.loc[i, 'TradeCode'] = -2
                if any([item in ordersTable.loc[i, 'Name'] for item in ['油气']]): #do not buy any 油气 etc.
                    if ordersTable.loc[i, 'TradeCode'] in [1, 1.0, 3, 3.0]: #disable buying
                        ordersTable.loc[i, 'TradeCode'] = -2 
                money = ordersTable.loc[i, 'Price'] * ordersTable.loc[i, 'Amount']
                if ordersTable.loc[i, 'TradeCode'] in [1, 1.0]: #cash
                    if maxCash - money <= 0:
                        ordersTable.loc[i, 'TradeCode'] = -2 
                    else:
                        maxCash -= money
                if ordersTable.loc[i, 'TradeCode'] in [3, 3.0]: #credit
                    if maxCredit - money <= 0:
                        ordersTable.loc[i, 'TradeCode'] = -2 
                    else:
                        maxCredit -= money
                
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
        df_lastTasks['Price'] = df_lastTasks['Price'].apply(float)
        df_lastTasks['Amount'] = df_lastTasks['Amount'].apply(float)
        df_lastTasks['Amount'] = df_lastTasks['Amount'].apply(int)

        for i in range(len(df_lastTasks)):
            stockCode   = df_lastTasks.loc[i, 'Code']
            targetPrice = round(float(df_lastTasks.loc[i, 'Price']), 3)
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
            if costPrice/currentPrice < 1.06: #make order withoin 6% margin
                price = max(currentPrice*0.96, costPrice) #make sure to sell but with fail price
                priceDiff = price/currentPrice-1
                remark = '趋势卖'
            else:
                price = currentPrice
                tradeCode = -1
                priceDiff = 0
                remark = '无操作'
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
    
    def buildTargetValueTasks(self, ordersTable, targetDate):
        self.write_log('Target date {}, today {}, tomorrow {}'.format(\
         dateTimeToDateStr(targetDate), getTodayDateStr(),
         getTomorrowDateStr()))
        orderTableHeaders = list(ordersTable.columns)
        nonSupportedList = list(ordersTable['NonSupported'])# margin buying not supported list
        nonSupportedList = [numberToStr(e) for e in nonSupportedList if (e is not None and e != 'None')]
        targetValueSheetNames = ['目标市值两融', '目标市值']
        newOrdersTable = pd.DataFrame(columns=orderTableHeaders)
        targetTasks = []
        for sheetname in targetValueSheetNames:
            sht = self.wb.sheets[sheetname]
            df  = sheetToDF(sht)
            df.loc[:, '下期时间'] = df.loc[:, '下期时间'].apply(numberToDateTime)
            df2 = df[df['下期时间']<=targetDate].copy().reset_index(drop=True)
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
        def p2f(x):
            return float(x.strip('%'))/100
        if p2f(self.db_sql.getValue('平台账本', 'G', 33)) > 0.04:
            self.msg.append('目标市值场外版临时操作')
        if p2f(self.db_sql.getValue('平台账本', 'G', 34)) > 0.04:
            self.msg.append('目标市值均值回归临时操作')
        tomorrowDate = getTodayDate()+timedelta(days=1)
        date1 = self.db_sql.getValue('目标市值场外版', 'U', 2)
        date1 = dateStrToDateTime(date1)
        date2 = self.db_sql.getValue('目标市值均值回归', 'V', 2)
        date2 = dateStrToDateTime(date2)
        if tomorrowDate ==  date1:
            self.msg.append('目标市值场外版定时操作')
        if tomorrowDate ==  date2:
            self.msg.append('目标市值均值回归定时操作')
        df3 = self.db_sql.getDF('备忘录')
        df3 = df3[['A', 'B']]
        df3.columns = df3.iloc[0].to_list()
        df3 = df3[df3.index>0]
        df3['日期'] = pd.to_datetime(df3['日期'], errors='coerce')
        df3 = df3.dropna(subset=['日期'])
        df3_task = df3[df3['日期'] == tomorrowDate].copy()
        if len(df3_task) > 0:
            self.msg.append('备忘录:')
            self.msg.append(df3_task.to_string(index=False))
        df4 = self.db_sql.getDF('梦想单')
        df4 = removeMargin(df4)
        df4.columns = df4.iloc[0]
        df4 = df4[1:]
        df4 = df4[['名称', '代码', '当前价格', '目标价格', '目的', '趋势', '满足条件']]
        df4['满足条件'] = df4['满足条件'].apply(lambda x: x.lower() == 'true')
        df4_task = df4[df4['满足条件']]
        if len(df4_task) > 0:
             self.msg.append('梦想单:')
             self.msg.append(df4_task.to_string(index=False))
    
    def getOrderedTasks(self, ordersTable):
        return ordersTable[ordersTable['TradeCode'] > 0].copy().reset_index(drop=True)
    
    def writeOrderedTasks(self, orderedTable):
#        orderedTable = ordersTable[ordersTable['TradeCode'] > 0].copy().reset_index(drop=True)
        targetCols = orderedTable.columns[:9].to_list()
        orderedTable = orderedTable.loc[:, targetCols]
        sht = self.wb.sheets['Ordered']
        sht.clear_contents()
        sht.range('A1').options(index=False).value = orderedTable
        sht.range('K1').value = '准备时间'
        sht.range('K2').value = '下单时间'
        sht.range('L1').value = getTodayDate()
        sht.range('M1').value = time.strftime("%H:%M:%S")

            
    def writeTradedTasks(self, df_lastTasks_traded):
#        orderedTable = ordersTable[ordersTable['TradeCode'] > 0].copy().reset_index(drop=True)
        # if len(df_lastTasks_traded) > 0 :
            # targetCols = orderedTable.columns[:9].to_list()
            # orderedTable = orderedTable.loc[:, targetCols]
        self.write_log('writeTradedTasks')
        sht = self.wb.sheets['Traded']
        sht.clear_contents()
        sht.range('A1').options(index=False).value = df_lastTasks_traded
        sht.range('J1').value = '确认时间'
        sht.range('K1').value = getTodayDate()
        sht.range('L1').value = time.strftime("%H:%M:%S")
            
    def getTotalValue(self):
        if self.WBInitialised:
            sht = self.wb.sheets['平台账本']
            self.totalValueFromSht = round(sht.range('O27').value, 2)
        else:
            self.totalValueFromSht = float(self.db_sql.getValue('平台账本', 'O', 27))
            
    
    def hasTradedToday(self):
        today = getTodayDate()
        return dateStrToDateTime(self.db_sql.getValue('Traded', 'K',1)) == today
    
    def hasUpdatedOrdersToday(self):
        today = getTodayDate()
        return dateStrToDateTime(self.db_sql.getValue('Ordered', 'L',1)) == today
    
    def hasOrderedToday(self):
        flag = False
        today = getTodayDate()
        orderedTime = self.db_sql.getValue('Ordered', 'L',2)
        if len(orderedTime)>0:
            if dateStrToDateTime(orderedTime) == today:
                flag = True
        return flag
    
    def prepareDFImgs(self):
        if self.hasTradedToday():
            _, df_traded = getDFFromDB(self.db_sql, 'Traded', 0, 8)
            if len(df_traded)== 0:
                df_traded.loc[0, :] = 'None'
            self.imgDict['tradedTable'] = dfToImg(df_traded)
        if self.hasOrderedToday():
            _, df_ordered = getDFFromDB(self.db_sql, 'Ordered', 0, 9)
            self.imgDict['orderedTable'] = dfToImg(df_ordered)
    
    def sendSummary(self):
        self.reminder()
        self.getTotalValue()
        self.prepareDFImgs()
        imgOrderKeys = ['Summary', 'tradedTable', 'Seperator', 'orderedTable', 'orderConfirmed']
        self.imgDict['Seperator'] = strToImg(''.join(['\n']+['-'*100]+['\n']))
        matchedFlag = True
        if self.totalValueFromImg > 0:
            if  self.totalValueFromImg == self.totalValueFromSht or \
                abs(self.totalValueFromImg-self.totalValueFromSht)<0.1:
                msg1 = 'Values are matched'
                self.msg.append(msg1)
            else:
                msg1 = 'Alert: Sht {}, Platform {} are not matched!'.format(self.totalValueFromSht, self.totalValueFromImg)
                self.msg.append(msg1)
                matchedFlag = False
                #imgOrderKeys.insert(1, 'totalValue')
        else:
            msg1 = 'Sht {}'.format(self.totalValueFromSht)
            self.msg.append("Platform is not checked this time.")
        self.imgDict['Summary'] = strToImg(msg1)
        if self.successfulTrading == -1:
            self.msg.append("Huatai platform is not used")
        elif self.successfulTrading == 0:
            self.msg.append("Orders are made successfully")
        else:
            self.msg.append("Failed to make orders")
        
        if len(self.imgDict) > 0:
            imgPath = os.path.join(os.getcwd(), 'Task'+getTodayDateStr()+'.png')
            imgs = [self.imgDict[key] for key in imgOrderKeys if key in self.imgDict]
            if len(imgs) > 1:
                img = mergeImg(imgs)
                from SelfTradingSystem.core.huataiPlatform2 import trim2
                img = trim2(img)
                img.save(imgPath)
                self.imgPath = imgPath
        msg = ''
        for m in self.msg:
            msg = msg + m + '\n'
        if len(self.imgPath) > 0 and not matchedFlag:
            sendEmail('今日汇总', msg, 'chenjiayi_344@hotmail.com', self.imgPath)
        else:
            sendEmail('今日汇总', msg, 'chenjiayi_344@hotmail.com')
    
    def updateShtFromPlatform(self, operator):
        sht = self.wb.sheets['平台账本']
        todayDate = getTodayDate()
        sht.range('G22').value = operator['cash']
        sht.range('G24').value = -1*operator['debit']
        sht.range('Q26').value = operator['credit']
        sht.range('Q22:Q24').value = todayDate
        
    def updateDBFromPlatform(self, operator):
        try:
            modifications = [('平台账本', 'G', 22, str(round(operator['cash'], 2))),
                             ('平台账本','G', 24, str(round(-1*operator['debit'], 2))),
                             ('平台账本','Q', 26, str(round(operator['credit'], 2))),
                             ('平台账本','Q', 27, str(round(operator['credit_cash'], 2))),
                             ]
            df_ordered = self.db_sql.getDF('Ordered')
            if 'orderedTable' in operator:
                df_ordered_new = operator['orderedTable']
                df_ordered_new2 = dfToDatabaseDF(df_ordered_new, df_ordered.columns)
                df_ordered_new2.loc[0, ['K', 'L', 'M']] = df_ordered.loc[0, ['K', 'L', 'M']].copy()
                df_ordered_new2.loc[1, ['K', 'L', 'M']] = df_ordered.loc[1, ['K', 'L', 'M']].copy()
                df_ordered_new2.loc[1,  'L'] = dateTimeToDateStr(getTodayDate())
                df_ordered_new2.loc[1,  'M'] = time.strftime("%H:%M:%S")
                df_ordered_new2 = df_ordered_new2.fillna('')
                self.db_sql.resetSubject('Ordered', df_ordered_new2)
            else:       
                modifications.append(('Ordered', 'L', 2, dateTimeToDateStr(getTodayDate())))
                modifications.append(('Ordered', 'M', 2, time.strftime("%H:%M:%S")))
               
            self.db_sql.modifySubjects(modifications)
        except:
            self.msg.append("Failed to updateDBFromPlatform")
        
    def readFinanceFromDB(self):
        try:
            self.credit_account['debit'] = float(self.db_sql.getValue('平台账本', 'G',24))*-1
            self.credit_account['credit'] = float(self.db_sql.getValue('平台账本', 'Q',26))
            self.credit_account['credit_cash'] = float(self.db_sql.getValue('平台账本', 'Q',27))
        except:
            self.msg.append("Failed to readFinanceFromDB")
    
    def updateForStreamlit(self):
        convertShtToDB(self.xlsxName)
        pass
    
    
    def cleanShtBatch(self):
        self.calculate()
        self.cleanSht('目标市值', 'AX', 'BL', 'AU10', 'AU24')
        self.cleanSht('目标市值两融', 'AX', 'BN', 'AU10', 'AU23')
        self.calculate()
    
    def cleanSht(self, sheetname, colStart, colEnd, feeLoc, feeSumLoc):
        sht = self.wb.sheets[sheetname]
        fee = sht.range(feeLoc).value
        if sht.range(feeSumLoc).value is None:
            sht.range(feeSumLoc).value = 0
        valid_list = []
        numRows = sht.range('A1').current_region.last_cell.row
        for i in range(2, numRows+1):
            cell = sht.range('A'+str(i)).value
            if cell is not None:
               valid_list.append(cell) 
            else:
               break
        numRows = sht.range(colStart+str(1)).current_region.last_cell.row
        for i in range(2, numRows+1):
            cell = sht.range(colStart+str(i)).value
            if cell is None:
                break
            if cell not in valid_list:
              rangeStr = "{colStart}{i}:{colEnd}{i}".format(colStart=colStart, colEnd=colEnd, i=i)
              print('Remove {} range {} {}'.format(sheetname, rangeStr, cell))
              sht.range(rangeStr).api.Delete(xw.constants.DeleteShiftDirection.xlShiftUp)

        self.calculate()
        sht.range(feeSumLoc).value += fee - sht.range(feeLoc).value 
        

    def calculate(self):
        if self.WBInitialised:
            self.wb.app.calculate()
    
    def save(self):
        if self.WBInitialised:
            self.wb.save()
    
    def close(self):
        # self.wb.app.kill()
        if self.WBInitialised:
            if len(self.wb.app.books) != 1:
               self.wb.close()
            # close excel application if only one workbook is open
            else:
                excel_app = xw.apps.active
                excel_app.quit()
        self.WBInitialised = False

def printTable(table):
    msg = tb.tabulate(table.values, table.columns, tablefmt="pipe")
    print(msg)
    return msg

def summaryTraded(sysObj, df_lastTasks_traded):
    targetCols = df_lastTasks_traded.columns[[0,1,3,4,5,7,8, -1]]
    df_lastTasks_traded = df_lastTasks_traded.loc[:, targetCols]
    df_lastTasks_traded['Price'] = df_lastTasks_traded['Price'].apply(lambda x: round(x, 3))
    df_lastTasks_traded['PriceDiff'] = df_lastTasks_traded['PriceDiff'].apply(lambda x: x if type(x) is str else '{}%'.format(round(x*100, 2))) 
    sysObj.write_log('These traded tasks are written:')
    msg = printTable(df_lastTasks_traded)
    sysObj.write_log(msg)
    sysObj.writeTradedTasks(df_lastTasks_traded)
            # sysObj.imgDict['tradedTable'] = dfToImg(df_lastTasks_traded)

def callBatchMethod(sysObj, methodStr):
    loopGuard = 3
    returnCode = 1
    while loopGuard > 0:
        try:
            sysObj.batchMethods[methodStr].__call__()
            sysObj.write_log("Updating {} done".format(methodStr))
            returnCode = 0
            break
        except:
            loopGuard -= 1
            sysObj.write_log("Something is wrong during calling {}, try {} times".format(methodStr, loopGuard))
            returnCode = -1
    return returnCode
 
'''
    subprocess.run(["D:\\Dropbox\\For daily life\\Investment\\RunHuatai.exe"])    
    from SmartQ_Python import SmartQ_Python
    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
    SmartQ_Python(ordersTable)
'''

def debuggingTradedTasks(df_lastTasks_traded, orderedTable_new):
    if df_lastTasks_traded is None or orderedTable_new is None:
        return
    if len(df_lastTasks_traded) > 0 and len(orderedTable_new) > 0:
        def f(x):
            if x.TradeCode in [1, 3]:
                return int(x.Amount)
            else: 
                return -1*int(x.Amount)
        orderedTable_new['Amount'] = orderedTable_new.apply(f, axis=1)
        df_new = orderedTable_new.loc[:, ['Name', 'Price', 'Amount', 'Remark']]
        df_traded = df_lastTasks_traded.loc[:, ['Name', 'Price', 'Amount', 'Remark']]
        df_new['Price'] = df_new['Price'].apply(lambda x: round(x, 3))
        df_traded['Price'] = df_traded['Price'].apply(lambda x: round(x, 3))
        list_new = df_new.values.tolist()
        list_traded = df_traded.values.tolist()
        has_duplicated = [l in list_new for l in list_traded]
        if any(has_duplicated):
            msg  = 'Traded:\n'
            msg += printTable(df_traded)
            msg += '\nNew ordered:\n'
            msg += printTable(df_new)
            sendEmail('Debugging: traded and ordered again', msg, 'chenjiayi_344@hotmail.com')
        pass
    # df_target = df_lastTasks_traded[(df_lastTasks_traded['Name'] == target) & (df_lastTasks_traded['Remark'] == strategy)]
    # if len(df_target) > 0:
        # sendEmail('Debugging', target+strategy, 'chenjiayi_344@hotmail.com')

def checkLastOrdered(sysObj, weekday=1, afterEarlySummary=False):
    sysObj.batchMethods = {}
    sysObj.batchMethods['updateMomentums'] = sysObj.updateMomentums
    sysObj.batchMethods['updateRelativeMomentums'] = sysObj.updateRelativeMomentums
    sysObj.batchMethods['calculateForMomentumShares'] = sysObj.calculateForMomentumShares
    sysObj.batchMethods['updateStockSheetLive'] = sysObj.updateStockSheetLive
    sysObj.batchMethods['updateFundSheetLiveFromTencent'] = sysObj.updateFundSheetLiveFromTencent
    sysObj.batchMethods['updateGoldPrice'] = sysObj.updateGoldPrice
    exitCode = 0
    df_lastTasks_traded = None
    t = time.time()
    if sysObj.hasTradedToday():
        if not sysObj.hasUpdatedOrdersToday() or afterEarlySummary:
            sysObj.initialSubjects()
            # callBatchMethod(sysObj, 'updateFundSheetLiveFromTencent')
            sysObj.updateFundSheetLiveFromTencent()
            sysObj.calculate()
        return exitCode, df_lastTasks_traded
    try:
        sysObj.initialSubjects()
        isTradingTime = sysObj.atTradingTime()
        if not isTradingTime:
## Part 1:
            sysObj.write_log("Updating not at trading time")
            nowTimeStru        = datetime.now().timetuple()
            if nowTimeStru.tm_wday != 6 and not afterEarlySummary:
                sysObj.readFinanceFromDB()
                hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
            else:
                hasTraded = False
            # hasTraded = False 
            if hasTraded: 
                summaryTraded(sysObj, df_lastTasks_traded)
                updateTradedTasks(sysObj, df_lastTasks_traded)
            else:
                sysObj.write_log("Did not have traded for today")
            callBatchMethod(sysObj, 'updateFundSheetLiveFromTencent')
            sysObj.calculate()
            
## Part 2:
            # callBatchMethod(sysObj, 'updateSheetsV2')
#            sysObj.momentum28Trade()
#            sysObj.write_log("Updated for momentum 28 trade")
            # sysObj.momentumIndustry()
            # sysObj.write_log("Updated for momentum industry")
#            if weekday != 5:
#                sysObj.momentumDayKTrade()
#                sysObj.write_log("Updated for momentum day K trade")
            if sysObj.atWeekend():
                sysObj.write_log("Have to update momentum at weekends")
                callBatchMethod(sysObj, 'updateMomentums')
                callBatchMethod(sysObj, 'updateRelativeMomentums')
            else:
                sysObj.write_log("Do not update momentum at weekdays")
            exitCode, tasks_valided = sysObj.calculateForMomentumShares() #write momentum shares
            if exitCode == -1: #move momentum share to sheets['趋势份额']
                writeMomentTasks(sysObj, tasks_valided)
                sysObj.calculate()
                sysObj.write_log("Calculating done 1")
            callBatchMethod(sysObj, 'updateGoldPrice')
            sysObj.calculate()
            sysObj.write_log("Calculating done 2")
#            sysObj.save()
#            sysObj.write_log("Saving done 2")
        else:
            sysObj.write_log("During trading time")
            callBatchMethod(sysObj, 'updateStockSheetLive')
            sysObj.calculate()
            sysObj.write_log("Calculating done 3")
#            sysObj.save()
#            sysObj.write_log("Saving done 3")
        t_usage = time.time() - t
        finishingMessage = "All tasks are finished in {:.2f} seconds".format(t_usage)
        sysObj.write_log(finishingMessage)       
        #autopy.alert.alert(finishingMessage, "Trading System")     
    except SettingWithCopyError:
        sysObj.write_log('handling..')
        frameinfo = getframeinfo(currentframe())
        sysObj.write_log(frameinfo.lineno)
    return exitCode, df_lastTasks_traded

def updatingOrders(sysObj, targetDate, flagForced=False):
    exitCode = 0
    if not sysObj.hasUpdatedOrdersToday() or flagForced:
        sysObj.initialSubjects()
        sysObj.write_log('updatingOrders buildTargetValueTasks targetDateStr {}'.\
              format(dateTimeToDateStr(targetDate)))
        ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
        ordersTable = sysObj.buildTargetValueTasks(ordersTable,targetDate)
        ordersTable = sysObj.buildMomentumTasks(ordersTable)
        ordersTable = sysObj.removeInvalidTasks(ordersTable)
        orderedTable = sysObj.getOrderedTasks(ordersTable)
        targetCols   = orderedTable.columns[[0,1,2,3,4,5,7,8 ]]
        orderedTable_new = orderedTable.loc[:, targetCols]
        orderedTable_new['Price'] = orderedTable_new['Price'].apply(lambda x: round(x, 3))
        orderedTable_new['PriceDiff'] = orderedTable_new['PriceDiff'].apply(lambda x: '{}%'.format(round(x*100, 2)))
        sysObj.write_log('These tasks are prapared for order:')
        printTable(orderedTable_new)
    
        # requiredCredit = 0
        # requiredCash   = 0
        # sht = sysObj.wb.sheets['平台账本']
        # availableCredit = round(sht.range('Q26').value, 2)
        # availableCash   = round(sht.range('Q27').value, 2)
        # for i in range(len(orderedTable)):
        #     if orderedTable.loc[i, 'TradeCode'] == 3:
        #         requiredCredit += round(orderedTable.loc[i, 'Amount']*orderedTable.loc[i, 'Price'], 2)
        #     elif orderedTable.loc[i, 'TradeCode'] == 1:
        #         requiredCash += round(orderedTable.loc[i, 'Amount']*orderedTable.loc[i, 'Price'], 2)
        # if requiredCredit + requiredCash > availableCredit + availableCash:
        #     msg  = "Required credit {} and cash {}, but available credit {} and cash {}"\
        #     .format(requiredCredit, requiredCash, availableCredit, availableCash)
        #     sendEmail('Alert', msg, 'chenjiayi_344@hotmail.com')
        #     return -1, sysObj
        # else:
        sysObj.writeOrderedTasks(orderedTable)
                #sysObj.sendSummary()
    return exitCode, sysObj

def getDFFromDB(db_sql, sht_name, colNum_start, colNum_end):
    orderedSht = db_sql.getDF(sht_name)
    columnLetters = [getColumnStr(i+1) for i in range(colNum_start, colNum_end)]
    orderedTable = orderedSht[columnLetters].copy()
    columns = orderedTable.head(1).values[0]
    orderedTable.truncate(after=0)
    orderedTable = orderedTable.truncate(before=1).reset_index(drop=True)
    orderedTable.columns = columns
    nan_value = float("NaN")
    orderedTable['Name'].replace("", nan_value, inplace=True)
    orderedTable.dropna(subset = [columns[0]], inplace=True)
    return orderedSht, orderedTable

# def makeOrders(sysObj):
#     _, orderedTable = getDFFromDB(sysObj.db_sql, 'Ordered', 0, 9)
#     orderedTable['TradeCode'] = orderedTable['TradeCode'].apply(float)
#     orderedTable['Price'] = orderedTable['Price'].apply(float)
#     orderedTable['Amount'] = orderedTable['Amount'].apply(float)
#     availableCredit = float(sysObj.db_sql.getValue('平台账本', 'Q', 26)) 
#     sysObj.totalValueFromSht = float(sysObj.db_sql.getValue('平台账本', 'O', 27))
#     exitCode, sysObj = operation(sysObj, orderedTable, availableCredit)
#     if exitCode == 0:
#         #orderedSht.loc[1, 'L'] = dateTimeToDateStr(getTodayDate())
#         #orderedSht.loc[1, 'M'] = time.strftime("%H:%M:%S")
#         #sysObj.db_sql.resetSubject('Ordered', orderedSht)
#         dateStr = dateTimeToDateStr(getTodayDate())
#         timeStr =  time.strftime("%H:%M:%S")
#         modifications = [('L', 2, dateStr), ('M', 2, timeStr)]
#         sysObj.db_sql.modifySubject('Ordered', modifications)
#     else:
#         sysObj.write_log("Error in SmartQ 2")
#     return exitCode, sysObj

            

def runRoutine(weekday=1, afterEarlySummary=False, margin_buying_disabled=False):
    exitCode = 0 
    sql = Database('Resources.db')
    sysObj = Trade('本金账本.xlsx', sql=sql, margin_buying_disabled=margin_buying_disabled)
    if sysObj.margin_buying_disabled:
        sysObj.msg.append("Margin buying disabled")
    if weekday == 5 and not afterEarlySummary:#update at friday's noon
        sysObj.msg.append("Weekly summary routine")
        from SelfTradingSystem.core.huataiPlatform2 import loginN
        app, operator = loginN(sysObj.pywinauto_app)
        app.kill(soft=False)#Avoid open platform and excel at the same time
        sysObj.initialSubjects()
        sysObj.updateShtFromPlatform(operator)
        sysObj.totalValueFromImg = operator['totalValue']
        
    exitCode, df_lastTasks_traded = checkLastOrdered(sysObj, weekday, afterEarlySummary)
    if exitCode != 0:
        raise Exception("Error in checkLastOrdered \n")
    sysObj.write_log("Updating orders now")
    targetDate = getTomorrowDate()
    exitCode, sysObj = updatingOrders(sysObj, targetDate)
    # debuggingTradedTasks(df_lastTasks_traded, orderedTable_new)
    if exitCode != 0:
        raise Exception("Error in updatingOrders \n")
    
    if sysObj.WBInitialised:
        sysObj.calculate()
        # raise Exception("Debug for different traded result")
        sysObj.save()
        sysObj.close() #Autoclose only when confirmaton is implemented
        sysObj.write_log("Closing done 3")
        sysObj.updateForStreamlit()

    sleep(5)
    if weekday != 5:
        if not sysObj.hasOrderedToday():
            sysObj.write_log("Ordering now")
            from SelfTradingSystem.core.huataiPlatform2 import loginAndOrder
            exitCode, sysObj, operator = loginAndOrder(sysObj)# Save and Close during ordering
            if exitCode == 0:
                sysObj.updateDBFromPlatform(operator)
                sleep(10)
            else:
                # sysObj.write_log("Error in SmartQ 3")
                raise Exception("Error in SmartQ 3")
        else:
            sysObj.write_log("Made orders already")
            
    sysObj.sendSummary()
    # sysObj.updateForStreamlit()
    return exitCode

if __name__ == '__main__':
    # from system import getTargetTradingTime
    # logFile = 'log.txt'
    margin_buying_disabled = True
    # lastTradedTime = getLastTradedTime(logFile)
    # targetTradeTime = getTargetTradingTime(lastTradedTime)
    # exitCode = runRoutine(weekday=1, margin_buying_disabled=margin_buying_disabled)
    # exitCode = runRoutine(weekday=5, afterEarlySummary=True, margin_buying_disabled=margin_buying_disabled)


    # exitCode = runRoutine()
    db_path = 'Resources.db'
    sql = Database(db_path)
    # sql.createDB(xlsx_path, db_path)
    # sysObj.write_log(sql.getLastRows('S000985', 10))
    # sleep(5)
    # sql.start()
    # sleep(5)
    # xlsx_path = r'D:\Downloads\本金账本.xlsx'
    sysObj = Trade('本金账本.xlsx',sql, margin_buying_disabled=True)
    # _, df_ordered = getDFFromDB(sysObj.db_sql, 'Ordered', 0, 9)
    # from SelfTradingSystem.core.huataiPlatform2 import makeReadyForTrade
    # makeReadyForTrade()
    # exitCode, sysObj = orderingOnly(sysObj)

    # checkLastOrdered(sysObj, weekday=1,afterEarlySummary=False)
    # exitCode = runRoutine(weekday=5, afterEarlySummary=False, margin_buying_disabled=False)
    # # sysObj.updateMomentums()
    # # from SelfTradingSystem.core.huataiPlatform2 import makeReadyForTrade
    # # makeReadyForTrade()
    # # exitCode, sysObj = orderingOnly(sysObj)
    # # checkLastOrdered(sysObj)
    
    # sysObj.updateForStreamlit()
    # sht = sysObj.wb.sheets['目标市值两融']
    # for row in range(2, 382):
    #     cell = sht.range('AZ'+str(row))
    #     if type(cell.value) == str:
    #         cell.value = datetime.strptime(cell.value, "%d/%m/%Y")
    #         sysObj.write_log('*'+cell.number_format)
    sysObj.initialSubjects()
    # sysObj.cleanShtBatch()
    # sysObj.updateForStreamlit()
    hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
    summaryTraded(sysObj, df_lastTasks_traded)
    # df = sysObj.getLastTasks()
    # exitCode = checkLastOrdered(sysObj, weekday=1, afterEarlySummary=False)
    # targetDate = getTomorrowDate()
    # updatingOrders(sysObj, targetDate)
    # df_lastTasks = sysObj.getLastTasks()
  
    # ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
    # ordersTable = sysObj.buildTargetValueTasks(ordersTable,targetDate)
    # ordersTable = sysObj.buildMomentumTasks(ordersTable)
    # ordersTable = sysObj.removeInvalidTasks(ordersTable)
    # orderedTable = sysObj.getOrderedTasks(ordersTable)
    # sysObj.updateStockSheetLive()
    # sysObj.updateFundSheetLiveFromTencent()
    # sysObj.updateMomentums()
    # sysObj.updateRelativeMomentums()
    
    # checkLastOrdered(sysObj, weekday=1)
    # sysObj.sendSummary()


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
