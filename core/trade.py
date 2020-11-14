# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 08:25:32 2019

@author: Frank
This is a Class file for TradeSubject:
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
from addLibraries import Helper
import xlwings as xw
import urllib
import urllib.error as error
import lxml
from lxml import html
import time
from datetime import datetime
from datetime import timedelta
#import pytz
#from dateutil.relativedelta import relativedelta
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
import re
import math
import itertools
import pandas as pd
import pdpipe as pdp
#import autopy
import subprocess
from socket import timeout
import numpy as np
#from addLibraries import Helper
from inspect import currentframe, getframeinfo
from pandas.core.common import SettingWithCopyError
pd.options.mode.chained_assignment = 'raise'
from dateutil.relativedelta import relativedelta
from StockHelper import BBI, MACD, MAs, AMAs
import ExcelHelper
import tabulate as tb
from PIL import Image
from SmartQ_PythonV2 import SmartQ_Python, dfToImg

def mergeImg(images):
    widths, heights = zip(*(i.size for i in images))
    total_height = sum(heights)
    max_width = max(widths)
    new_im = Image.new('RGB', (max_width, total_height), color=(255,255,255))
    y_offset = 0
    for im in images:
      new_im.paste(im, (0,y_offset))
      y_offset += im.size[1]
    return new_im


def sleep(seconds): #for KeyboardInterrupt 
    for i in range(seconds):
        time.sleep(1)
        
def resetZZQZWithXLSXName(xlsxName, sht_new_df):
    sysObj = TradeSystem(xlsxName)
    sysObj.initialSubjects()
    subObj = sysObj.objMap['S000985']
    df = sheetToDF(subObj.sht)
    startIdx = list(df['名称'].values)
    startIdx = [i for i in range(len(startIdx)) if type(startIdx[i]) != str]
    if len(startIdx) > 0:
        startIdx = startIdx[0]
        startDate = dateStrToDateTime(numberToDateStr(float(df.loc[startIdx, '日期'])))
        sht_appended = sht_new_df[sht_new_df[subObj.DateStr] >= startDate].copy()
        sht_appended[subObj.DateStr] = sht_appended[subObj.DateStr].apply(numberToDateStr)
        newContents = sht_appended.values.tolist()
        if len(newContents) > 0:
            subObj.sht.range(startIdx+2, 1).value = newContents
            sysObj.save()
    sysObj.close()
        

def resetZZQZ():
    todayDate = getTodayDate()
    startDate = todayDate - relativedelta(days=30)
    try:
        sht_new_df = getStockHistoryV2(buildStockNumberStr('000985'), startDate, todayDate)
        sht_new_df['日期'] = sht_new_df['日期'].apply(dateStrToDateTime) 
        resetZZQZWithXLSXName('指数和基金.xlsx', sht_new_df)
        resetZZQZWithXLSXName('本金账本.xlsx', sht_new_df)
    except:
        pass


def buildStockNumberStr(stockNumberStr):
    stockNumber = int(stockNumberStr)
    numDigits = len(str(stockNumber))
    if numDigits == 7:
        stockNumberStr = str(stockNumber)
    elif numDigits < 7:
        if numDigits < 6:
            extraLength = 7 - numDigits
            extraStr    = '0'*extraLength
        else:
            extraStr    = '1'
        stockNumberStr = extraStr + str(stockNumber)
    return stockNumberStr


def getLastTradedTime():
    with open('TradingSystemV4_log.txt', 'rt') as log:
        lines = log.readlines()
    if len(lines) == 0:
        raise Exception("No traded time found")
    else:
        lines = [line for line in lines if 'Success' in line]
        lastLine = lines[-1]
        lastTradedTimeStr = extractBetween(lastLine, 'Success at ', '\n')[0]
        lastTradedTime    = timeStrToDateTime(lastTradedTimeStr)
        return lastTradedTime

def extractBetween(string, sub1, sub2):
    numSub1 = len(sub1)
    numSub2 = len(sub2)
    dict_sub1 = {i : string[i:i+numSub1] for i in range(len(string)-numSub1+1)}
    dict_sub2 = {i : string[i:i+numSub2] for i in range(len(string)-numSub2+1)}
    ind_sub1 = [i for i in range(len(dict_sub1)) if dict_sub1[i] == sub1]
    ind_sub2 = [i for i in range(len(dict_sub2)) if dict_sub2[i] == sub2]
    numPars = min(len(ind_sub1), len(ind_sub2))
    results = [string[ind_sub1[i]+numSub1:ind_sub2[i]] for i in range(numPars)]
    return results

def getYearFromDate(dateStr):
    return dateStrToDateStruc(dateStr).tm_year

def getMonthFromDate(dateStr):
    return dateStrToDateStruc(dateStr).tm_mon

def getWeekNumFromDate(dateStr):
    return dateStrToDateTime(dateStr).strftime("%W")

def getStockNumberStr(stockStr):
    return stockStr[2:]

def indCell(colStr, rowNumber):
    if type(colStr) is str:
        return colStr+str(rowNumber)
    else:
        return chr(ord('A') + colStr -1)+str(rowNumber)
    
def dateStrToDateTime(dateStr):
    return datetime.strptime(dateStr, "%Y%m%d")

def dateStrToDateStruc(dateStr):
    return dateStrToDateTime(dateStr).timetuple()

def dateTimeToDateStr(date_time):
    return datetime.strftime(date_time, "%Y%m%d")

def dateTimeToEuroDateStr(date_time):
    return datetime.strftime(date_time, "%d/%m/%Y")


def datetimeToTimeStr(date_time):
    return datetime.strftime(date_time, "%Y%m%d_%H%M%S")

def timeStrToDateTime(timeStr):
    return datetime.strptime(timeStr, "%Y%m%d_%H%M%S")

def timeStrToDateStr(timeStr):
    return dateTimeToDateStr(timeStrToDateTime(timeStr))

def cellDateToEuroDateStr(cell_date):
    dateStr=dateTimeToDateStr(cell_date)
    return dateTimeToEuroDateStr(dateStrToDateTime(dateStr))

def getTodayDate():
    localTime =time.localtime()
    todayDateStr = str(localTime.tm_year) + str(localTime.tm_mon).zfill(2) + str(localTime.tm_mday).zfill(2)
    return dateStrToDateTime(todayDateStr)

def getTodayDateStr():
    return dateTimeToDateStr(getTodayDate())

def getTomorrowDateStr():
    return dateTimeToDateStr(getTomorrowDate())

def getTomorrowDate():
    todayDate = getTodayDate()
    return todayDate+timedelta(days=1)

def getDeltaDateStr(ndays):
    todayDate = getTodayDate()
    return dateTimeToDateStr(todayDate+timedelta(days=ndays))

def getNowTimeStr():
    return datetimeToTimeStr(datetime.now())
    
def getDaysBetweenDateStrs(dateStr1, dateStr2):
    date1 = dateStrToDateTime(dateStr1)
    date2 = dateStrToDateTime(dateStr2)
    return (date1-date2).days
    

def isnumeric(e):
    flag = False
    if e is not None and e != 'None':
        flag = True
    if type(e) == float:
        return not math.isnan(e)
    if type(e) == str:
        try:
            new_e = float(e)
            if not math.isnan(new_e):
                return isnumeric(float(e))
            else:
                flag = False
        except:
            flag = False
    return flag

def numberToStr(inputNumber):
    type_input = type(inputNumber)
    if type_input is str:
        return inputNumber
    elif type(inputNumber) is int:
        return numberToStr(float(inputNumber))
    elif isinstance(inputNumber, datetime):
        return dateTimeToDateStr(inputNumber)
    elif type(inputNumber) is float:
        if math.isnan(inputNumber):
            return ""
        else:
            numberStr = ""
            divisor = 100000
            for i in range(6):
                numberStr += str(int(inputNumber//divisor))
                inputNumber = inputNumber%divisor
                divisor /= 10
            return numberStr
    elif isinstance(inputNumber, lxml.etree._ElementUnicodeResult):
        return inputNumber.__str__()
    
    else:
        raise Exception("Non defined type {} for {} ".format(type(inputNumber), inputNumber))
        
def numberToDateTime(inputNumber):
    return dateStrToDateTime(numberToStr(inputNumber))

def numberToDateStr(inputNumber):
    return dateTimeToDateStr(dateStrToDateTime(numberToStr(inputNumber)))

def fundDateEleToDateStr(dateEle):
    dateStr = [str(s) for s in dateEle if str(s) != '-']
    return "".join(dateStr)
    
def getHTML(tempUrl, timeLimit=20):
    attempts = 5
    tempHTML = []
    while attempts > 0:
        try:
            tempHTML = urllib.request.urlopen(tempUrl, timeout=timeLimit)
            break
        except error.URLError as e:
            attempts -= 1
            print("Error {} during obtaing HTML {} remain attempts {}".format(e.reason, tempUrl, attempts))
        except timeout:
            attempts -= 1
            print("Time out during obtaing HTML{} remain attempts {}".format(tempUrl, attempts))
    return tempHTML        
            
def getSeasonNum(date):
    return date.tm_mon//4 + 1

def getIndexOfChar(string, char):
    indices = [];
    for i in range(len(string)):
        if (string[i] == char):
            indices.append(i)
    return indices

def splitStockStr(stockStr):
    indicesOfDoubleQuotation = getIndexOfChar(stockStr, '"')
    message = "The stock string is not double quotated: {}".format(stockStr)
    assert len(indicesOfDoubleQuotation) == 2, message
    indicesOfComma           = getIndexOfChar(stockStr, ',')
    result = []
    for i in range(len(indicesOfComma)):
        if(i==0):
            start = indicesOfDoubleQuotation[0]+1
            end   = indicesOfComma[0]
        else:
            start = indicesOfComma[i - 1] + 1
            end   = indicesOfComma[i]
        result.append(stockStr[start:end])
    return result

def getStock(stock):
    tempUrl = "http://hq.sinajs.cn/list=" + stock
    tempHTML = getHTML(tempUrl)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("gbk");
    stockStr_list = splitStockStr(tempStr)
    if len(stockStr_list) < 33:
        stockStr_list.append(''*(32-len(stockStr_list)))
    return stockStr_list

def getStocks(stocks):
    longStock = ",".join(stocks)
    tempUrl = "http://hq.sinajs.cn/list=" + longStock
    tempHTML = getHTML(tempUrl, timeLimit=40)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("gbk");
    tempStrsList = tempStr.split('\n')
    tempStrsList = [tempStr for tempStr in tempStrsList if len(tempStr) > 0]
    stockStrs_list= []
    for tempStr in tempStrsList:
        stockStr_list = splitStockStr(tempStr)
        if len(stockStr_list) < 33:
            stockStr_list.append(''*(32-len(stockStr_list)))
        stockStrs_list.append(stockStr_list)
    return stockStrs_list
    
def testSplitStockStr():
    stock = "sh513030"
    stockStr = getStock(stock)
    result = splitStockStr(stockStr)
    for value in result:
        print (value, end=", ")
        


        
def getFund(fundNumberStr, numPage=1, numDay=1, loopGuard=5):
    if type(fundNumberStr) == list:
        Temp       = fundNumberStr
        fundNumberStr = Temp[0]
        numPage    = Temp[1]
        numDay     = Temp[2]
    tempUrl = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={}&page={}&per={}'.format(fundNumberStr, numPage, numDay)
    tempHTML = getHTML(tempUrl)
#    tempHTML = urllib.request.urlopen(tempUrl)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("UTF-8")
    tree = html.fromstring(tempStr) 
    tables = [ e for e in tree.iter() if e.tag == 'table']
    eps_table = tables[-1]
    table_rows = [ e for e in eps_table.iter() if e.tag == 'tr']
    results = []
    for row in table_rows[1:]:
        cell_content = [ e.text_content() for e in row.iter() if e.tag == 'td']
        results.append(cell_content)
    if len(results[0]) < 4 and loopGuard > 0:
        loopGuard -= 1
        return getFund(fundNumberStr, numPage, numDay, loopGuard)
    else:
        if len(results[0]) < 4:
            raise ValueError("The content for {} is too short: {}".format(fundNumberStr, results))
        return results

def getFundHistory(fundNumberStr,  rows=0, pool=[]):
    defaultDayPerPage = 45
    tempUrl = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={}&page=1&per=1'.format(fundNumberStr)
    tempHTML = getHTML(tempUrl)
#    tempHTML = urllib.request.urlopen(tempUrl)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("UTF-8");
    tree = html.fromstring(tempStr)
    tables = [ e for e in tree.iter() if e.tag == 'table']
    eps_table = tables[-1]
    table_rows = [ e for e in eps_table.iter() if e.tag == 'tr']
    column_headings =[ e.text_content() for e in table_rows[0].iter() if e.tag == 'th']
    numTotalDay = int(re.search( 'records:(.*),pages', tempStr).group(1))
    if rows != 0:
        numTotalDay = rows
    numTotalPage = math.ceil(numTotalDay/defaultDayPerPage)
    tasks = [i+1 for i in range(numTotalPage)  ]
    tasks = zip([fundNumberStr]*numTotalPage, tasks, [defaultDayPerPage]*numTotalPage)
    tasks = [list(i) for i in tasks]
    if isinstance(pool, multiprocessing.pool.ThreadPool):
        results = pool.map(getFund, tasks)
    else:
        results = [getFund(task) for task in tasks]
    newResults = []
    for i in range(len(results)):
        newResults += results[i]
    finalResults = pd.DataFrame(newResults, columns=column_headings)
    finalResults = finalResults.iloc[::-1]
    finalResults.loc[:, '净值日期'] = finalResults.loc[:, '净值日期'].apply(fundDateEleToDateStr)
    finalResults.reset_index()
    return finalResults


def getStockHistroyFrom163(stockNumberStr, thisYear=1990, thisSeason=1):
    if type(stockNumberStr) is list:
        temp = stockNumberStr
        stockNumberStr = temp[0]
        thisYear       = temp[1]
        thisSeason     = temp[2]
    tempUrl = 'http://quotes.money.163.com/trade/lsjysj_zhishu_{}.html?year={}&season={}'.format(stockNumberStr, thisYear, thisSeason);
    tempHtml = getHTML(tempUrl)
    tempContent = tempHtml.read()
    tempStr = tempContent.decode("UTF-8");
    tree = html.fromstring(tempStr)
    tables = [ e for e in tree.iter() if e.tag == 'table']
    contents = tables[3]
    table_heading = extractBetween(tempStr, '<tr class="dbrow">\r\n', '</tr>\r\n        </thead>\r\n')
    headers = extractBetween(table_heading[0], '<th>', '</th>\r\n')
    table_rows = [ e for e in contents.iter() if e.tag == 'tr']
    results = []
    for row in table_rows[1:]:
        cell_content = [ e.text_content() for e in row.iter() if e.tag == 'td']
        results.append(cell_content)
    finalResults = pd.DataFrame(results, columns=headers)
    return finalResults
    
def getZZQZLatestMonth():
    url = 'http://www.csindex.com.cn/zh-CN/indices/index-detail/000985'
    tempHTML = getHTML(url, timeLimit=40)
    df = []
    if type(tempHTML) is not list:
        tempContent = tempHTML.read()
        tempStr = tempContent.decode("utf-8")
        rawStr = extractBetween(tempStr, 'var data = [];', 'var hqt =')
        rawStr = rawStr[0]
        rawList = rawStr.split('\r\n')
        datas = []
        dates = []
        for raw in rawList:
            if 'dates' in raw:
                dateStr = extractBetween(raw, ' "', '";')[0]
                date = dateTimeToDateStr(datetime.strptime(dateStr, "%Y-%m-%d"))
                dates.append(date)
            elif 'data' in raw:
                dataStr = extractBetween(raw, ' "', '";')[0]
                data = float(dataStr)
                datas.append(data)
        columns = ['日期','股票代码','名称','开盘价','最高价','最低价','收盘价','涨跌额','涨跌幅']
        df = pd.DataFrame(np.zeros([len(dates), len(columns)], dtype=float), columns=columns)
        df['日期'] =   dates  
        df['收盘价'] =  datas
    return df

def getStockHistoryV2(stockNumberStr, startDate=datetime.strptime('19990101', "%Y%m%d"), endDate=datetime.today()):
    tempUrl = 'http://quotes.money.163.com/service/chddata.html?code={}&start={}&end={}&fields=TOPEN;HIGH;LOW;TCLOSE;CHG;PCHG'.format(stockNumberStr, dateTimeToDateStr(startDate), dateTimeToDateStr(endDate))
    tempHtml = getHTML(tempUrl, timeLimit=40)
    tempContent = tempHtml.read()
    tempStr = tempContent.decode("gbk")
    line_list = tempStr.split('\r\n')
    header = line_list[0].split(',')
    data_row = [line_list[i].split(',') for i in range(1, len(line_list)-1)]
    df = pd.DataFrame(data=data_row, columns = header)
    df.loc[:, '日期'] = df.loc[:, '日期'].apply(lambda x : dateTimeToDateStr(datetime.strptime(x, "%Y-%m-%d")))
    return df.iloc[::-1].reset_index(drop=True)

def getStockHistory(stockNumberStr, startDate=datetime.strptime('19990101', "%Y%m%d"), endDate=datetime.today(), pool=[]):
    if stockNumberStr == '000985':
        return getStockHistoryV2(buildStockNumberStr('000985'), startDate, endDate)
    else:
        startYear = startDate.year
        endYear   = endDate.year
        seasons = [1, 2, 3, 4]
        tasks = []
        for y in range(startYear, endYear+1):
            task = list(zip([stockNumberStr]*4, [y]*4, seasons))
            task = [list(t) for t in task]
            tasks += task
        if isinstance(pool, multiprocessing.pool.ThreadPool):
            results = pool.map(getStockHistroyFrom163, tasks)
        else:
            results = [getStockHistroyFrom163(task) for task in tasks]
        results = [result.iloc[::-1] for result in results if len(result)>0]
        finalResults = pd.concat(results)     
        finalResults.reset_index()
        return finalResults
            


def updateStockSheet(sht_stock, stockNumberStr, sheetName, pool=[], resetFlag=False):
    if resetFlag:
        sht_stock.clear_contents()
        sht_stock_df = getStockHistory(stockNumberStr, pool=pool)
        sht_stock.range('A1').options(index=False).value = sht_stock_df
    else:
        numLastRow = sht_stock.range('A1').current_region.last_cell.row
        lastUpdateDateStr = numberToStr(sht_stock.range(numLastRow, 1).value)
        startDate=dateStrToDateTime(lastUpdateDateStr)
        if startDate < getTodayDate():
            sht_stock_new_df = getStockHistory(stockNumberStr, startDate=startDate, pool=pool)
            sht_stock_appended = sht_stock_new_df[sht_stock_new_df['日期'].map(dateStrToDateTime) > startDate]
            sht_stock.range(numLastRow+1, 1).value = sht_stock_appended.values.tolist()
    
def updateStockSheets(wb, stockNumberStrs, pool=[]):
    sheetNames = [wb.sheets[s].name for s in range(wb.sheets.count)]
    for stockNumberStr in stockNumberStrs:
        sheetName = [s for s in sheetNames if stockNumberStr in s]
        if len(sheetName) == 0:
            sheetName = 'S'+stockNumberStr
            wb.sheets.add(sheetName,after=wb.sheets['Menu'])
            sht_stock = wb.sheets[sheetName]
            updateStockSheet(sht_stock, stockNumberStr, sheetName, pool=pool, resetFlag=True)
        elif len(sheetName) == 1:
            sheetName = sheetName[0]
            sht_stock = wb.sheets[sheetName]
            if sht_stock.range('A2').value is None:
                updateStockSheet(sht_stock, stockNumberStr, sheetName, pool=pool, resetFlag=True)
            else:
                updateStockSheet(sht_stock, stockNumberStr, sheetName, pool=pool, resetFlag=False)
        else:
             raise Exception("Too many sheet names {} include {}".format(sheetName, stockNumberStr))
        
def updateFundSheet(sht_fund, fundNumberStr, sheetName, pool=[], resetFlag=False):
   if resetFlag:
        sht_fund.clear_contents()
        sht_fund_df = getFundHistory(fundNumberStr,pool=pool)
        sht_fund.range('A1').options(index=False).value = sht_fund_df
   else:
        numLastRow = sht_fund.range('A1').current_region.last_cell.row
        lastUpdateDateStr = numberToStr(sht_fund.range(numLastRow, 1).value)
        startDate=dateStrToDateTime(lastUpdateDateStr)
        if startDate < getTodayDate():
            diffDays = (getTodayDate() - startDate).days
            sht_fund_new_df = getFundHistory(fundNumberStr, rows=diffDays,pool=pool)
            sht_fund_appended = sht_fund_new_df[sht_fund_new_df['净值日期'].map(dateStrToDateTime) > startDate]
            sht_fund.range(numLastRow+1, 1).value = sht_fund_appended.values.tolist()

def updateFundSheets(wb, fundNumberStrs, pool=[]):
    sheetNames = [wb.sheets[s].name for s in range(wb.sheets.count)]
    for fundNumberStr in fundNumberStrs:
        sheetName = [s for s in sheetNames if fundNumberStr in s]
        if len(sheetName) == 0:
            sheetName = 'F'+fundNumberStr
            wb.sheets.add(sheetName,after=wb.sheets['Menu'])
            sht_fund = wb.sheets[sheetName]
            updateFundSheet(sht_fund, fundNumberStr, sheetName, pool=pool, resetFlag=True)
        elif len(sheetName) == 1:
            sheetName = sheetName[0]
            sht_fund = wb.sheets[sheetName]
            if sht_fund.range('A2').value is None:
                updateFundSheet(sht_fund, fundNumberStr, sheetName, pool=pool, resetFlag=True)
            else:
                updateFundSheet(sht_fund, fundNumberStr, sheetName, pool=pool, resetFlag=False)
        else:
             raise Exception("Too many sheet names {} include {}".format(sheetName, fundNumberStr))




def checkMomentum(df):
    df = BBI(df)
    df, TCloseStr = MACD(df)
    lastRow = df.tail(1)
    last_BBI = lastRow[ 'BBI'].iloc[-1]
    last_Price = lastRow[TCloseStr].iloc[-1]
    last_Bar   = lastRow['BAR'].iloc[-1]
    if last_BBI == 0:
        BBI_m = -1
    elif  last_BBI < last_Price:
        BBI_m = 1
    else:
        BBI_m = 0
    if last_Bar == 0:
        MACD_m = -1
    elif  last_Bar > 0:
        MACD_m = 1
    else:
        MACD_m = 0
    return BBI_m,  MACD_m


    
def sheetToDF(sht):
    return sht.range('A1').options(pd.DataFrame, 
                         header=1,
                         index=False, 
                         expand='table').value

#def getColumnStr(colNum):
#    letters = list(map(chr, range(ord('A'), ord('Z')+1)))
#    numLetter = len(letters)
#    colStr = []
#    loopGuard = 10
#    remainder = colNum
#    while loopGuard > 0:
#        loopGuard -= 0
#        if remainder <= numLetter:
#            colStr.append(letters[remainder-1])
#            break
#        quotient  = remainder//numLetter
#        colStr.append(letters[quotient-1])
#        remainder = remainder%numLetter
#    return colStr
    
def getColumnStr(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string                    

def round_up(n, decimals=0): 
    multiplier = 10 ** decimals
    if multiplier > 0:
        return math.ceil(n * multiplier) / multiplier
    else:
        return math.floor(n * multiplier) / multiplier

def round_down(n, decimals=0): 
    multiplier = 10 ** decimals 
    if multiplier > 0:
        return math.ceil(n * multiplier) / multiplier
    else:
        return math.floor(n * multiplier) / multiplier


def getTradingPlanAS(df_row, modifier):
    ind = df_row.index[0]
    openning_price = df_row.loc[ind, '开网价格']*modifier #I2
    openning_position = df_row.loc[ind, '开网金额'] #J2
    net_interval = df_row.loc[ind, '网眼大小']     #K2
    openning_net_num = df_row.loc[ind, '开网网数'] #L2
    position_modifier = df_row.loc[ind, '金额倍数'] #M2
    profit_modifier = df_row.loc[ind, '留利润倍数'] #N2
    max_net_num = int(round(1/net_interval))
    columns = ['net_num', 'buy_price', 'buy_position', 'buy_share', 'sell_price', 'sell_position', 'sell_share']
    tradingPlan = np.zeros([max_net_num, 7], dtype=np.float)#buy_price, buy_position, buy_share, sell_price, sell_position, sell_share
    for i in range(max_net_num):
        tradingPlan[i][0] = i+1
        tradingPlan[i][1] = round((openning_price/(1-net_interval*openning_net_num))*(1-net_interval*(i+1)), 3)
        tradingPlan[i][2] = round(openning_position*(1+position_modifier)**i, 2)
        if tradingPlan[i][1] > 0:
            tradingPlan[i][3] = round_up(tradingPlan[i][2]/tradingPlan[i][1], -2)
        tradingPlan[i][4] =  round((openning_price/(1-net_interval*openning_net_num))*(1-net_interval*(i-1)), 3)
        tradingPlan[i][5] = round(-1*(openning_position*(1+position_modifier)**(i-1))*(1-net_interval*profit_modifier), 2)
        if tradingPlan[i][4] > 0:
            tradingPlan[i][6] = round_down(tradingPlan[i][5]/tradingPlan[i][4], -2)
            
    df_plan = pd.DataFrame(data=tradingPlan, columns=columns)      
    return df_plan

def getTradingPlanDefault(df_row, modifier):
    ind = df_row.index[0]
    openning_price = df_row.loc[ind, '开网价格']*modifier #I2
    openning_position = df_row.loc[ind, '开网金额'] #J2
    net_interval = df_row.loc[ind, '网眼大小']     #K2
    openning_net_num = df_row.loc[ind, '开网网数'] #L2
    position_modifier = df_row.loc[ind, '金额倍数'] #M2
    profit_modifier = df_row.loc[ind, '留利润倍数'] #N2
    max_net_num = int(round(1/net_interval))
    columns = ['net_num', 'buy_price', 'buy_position', 'buy_share', 'sell_price', 'sell_position', 'sell_share']
    tradingPlan = np.zeros([max_net_num, 7], dtype=np.float)#buy_price, buy_position, buy_share, sell_price, sell_position, sell_share
    for i in range(max_net_num):
        tradingPlan[i][0] = i+1
        tradingPlan[i][1] = round(openning_price*(1+net_interval*(-i+openning_net_num-1)), 3)
        tradingPlan[i][2] = round((openning_position*(1+position_modifier)**i), 2)
        if tradingPlan[i][1] > 0:
            tradingPlan[i][3] = round_up(tradingPlan[i][2]/tradingPlan[i][1], -2)
        tradingPlan[i][4] =  round(openning_price*(1+net_interval*(1-i+openning_net_num)), 3)
        tradingPlan[i][5] = round(-1*(openning_position*(1+position_modifier)**(i-1))*(1-net_interval*profit_modifier), 2)
        if tradingPlan[i][4] > 0:
            tradingPlan[i][6] = round_down(tradingPlan[i][5]/tradingPlan[i][4], -2)
            
    df_plan = pd.DataFrame(data=tradingPlan, columns=columns)      
    return df_plan


def getTradingPlanMain(df_row,netType, modifier):
    if netType == '等差':
        return getTradingPlanAS(df_row, modifier)
    elif netType == '初始':
        return getTradingPlanDefault(df_row, modifier)

def getTargetValuePlan(df_row, momentumTargets):
    ind = df_row.index[0]
    name          = df_row.loc[ind, '基金名称']
#    tomorrow_date = getTomorrowDateStr()
    current_price =  df_row.loc[ind, '当前价格']
#    initial_price = float(df_row.loc[ind, '期初价格'])
    current_share = float(df_row.loc[ind, '持有份数'])
    target_value = df_row.loc[ind, '目标市值'] #I2
#    target_date = dateTimeToDateStr(df_row.loc[ind, '下期时间'])
    tempTradedStr = df_row.loc[ind, '临时操作记录']
    tempTradedSyms = tempTradedStr.split('/')
    tempSold   = tempTradedSyms[0] != '0'
    tempBought = tempTradedSyms[1] != '0'
    momentum_value = 0.0
    momentum_share = 0
    momentum_price = 0.000
    if '周' in name:
        return  momentum_share, momentum_value, momentum_price
    else:
        isLockBuy  = any(target in name for target in momentumTargets[0])
        isLockSell = any(target in name for target in momentumTargets[1])
        current_value = current_share*current_price
        if current_value > target_value * 1.20 and not tempSold and isLockSell:
            momentum_price = current_price#round(target_value*1.20/current_share, 3)
            momentum_share = -1*round_down((target_value*1.20-target_value)/momentum_price, -2)
        elif current_value < target_value*0.85 and not tempBought and isLockBuy:
            momentum_price = current_price#round(target_value*0.85/current_share, 3)
            momentum_share = round_down((target_value - target_value*0.85)/current_price, -2)
            momentum_value = momentum_price*momentum_share
#        elif target_date == tomorrow_date: # weekly trading has no momentum share
#            momentum_price = current_price
#            momentum_share = float(df_row.loc[ind, '调整份数'])
#            momentum_value = momentum_share*current_price
#            if current_value > target_value and isLockSell:
#                momentum_share = max(momentum_share, -1*round_down((current_value-target_value)/current_price, -2))
#            elif current_value < target_value and isLockBuy:
#                momentum_share = max(momentum_share, -1*round_down((current_value-target_value)/current_price, -2))
#            else:#Do not lock it at target date
#                momentum_value = 0.0
#                momentum_share = 0
#                momentum_price = 0.000
        return  momentum_share, momentum_value, momentum_price


def calculateForMomentumShareForNormal(wb, header, momentumTargets):
    sheetName = '普通网格'
    targetsList = momentumTargets[0] + momentumTargets[1]
    sht = wb.sheets['Preorders']
    df  = getTargetArea(sht, 'A', 'I')
    df = df[df['Remark'] == sheetName].copy().reset_index(drop=True)
    numRows = len(df)
    df_latestStockPrices = sheetToDF(wb.sheets['股票查询'])
    pipeline  = pdp.ApplyByCols('股票代码', getStockNumberStr, '股票代码', drop=False)
    df_latestStockPrices = pipeline(df_latestStockPrices)
    data = []
    for i in range(numRows):
        df_row = df.iloc[[i]].copy()
        ind = df_row.index[0]
        name   = df_row.loc[ind, 'Name']
        if any(target in name for target in targetsList):
            code   = numberToStr(df_row.loc[ind, 'Code'])
            date   = getTodayDate()
            price  = df_row.loc[ind, 'Price']
            amount = df_row.loc[ind, 'Amount']
            lockStatus = df_row.loc[ind, '锁仓']
            momentum_share = 0
            momentum_value = 0.0
            momentum_price = 0.000
            if len(lockStatus) > 0 and amount != 0:
                targetPrice = round(price, 3)
                momentum_share = amount
                price_open  = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == code, '今开'].values[0]
                price_max   = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == code, '最高'].values[0]
                price_min   = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == code, '最低'].values[0]
                if amount > 0:
                    if targetPrice >= price_min:
                        if targetPrice > price_open:
                            momentum_price = price_open
                        else:
                            momentum_price = targetPrice
                else: #amount != 0 
                    if targetPrice <= price_max:
                        if targetPrice < price_open:
                            momentum_price = price_open
                        else:
                            momentum_price = targetPrice
                if '卖' in lockStatus:
                    momentum_value = 0.0
                else:
                    momentum_value = round(price*amount, 2)
                data.append([name, code, date, momentum_share, momentum_price, momentum_value,sheetName])    
    task  = pd.DataFrame(data=data, columns=header)
    return task

def calculateForMomentumShare(wb,sheetName, header, momentumTargets):
    targetsList = momentumTargets[0] + momentumTargets[1]
    if sheetName == '普通网格':
        return calculateForMomentumShareForNormal(wb, header, momentumTargets)
    else:
        sht = wb.sheets[sheetName]
        df  = sheetToDF(sht)
        numRows = len(df)
        data = []
        for i in range(numRows):
            df_row = df.iloc[[i]].copy()
            ind = df_row.index[0]
            name   = df_row.loc[ind, '基金名称']
            if name == '全指医药ETF5':
                time.sleep(0.1)
            if any(target in name for target in targetsList):
                code   = numberToStr(df_row.loc[ind, '基金代码'])
                date   = df_row.loc[ind, '净值日期']
                momentum_share = 0
                momentum_value = 0.0
                momentum_price = 0.000
                if '网格' not in sheetName:
                    momentum_share, momentum_value, momentum_price = getTargetValuePlan(df_row, momentumTargets)
                else:
                    if sheetName == '简易网格':
                        netType = '等差'
                        modifier = df_row.loc[ind, '调整因子']
                    else:
                        netType = df.loc[i, '网格类型']
                        modifier = 1
                    df_plan = getTradingPlanMain(df_row,netType,modifier)
                    net_interval = df.loc[i, '网眼大小']
                    if net_interval < 0.15:
                        current_net_num = int(df.loc[i, '持有网数'])
                        current_price =  df.loc[i, '当前价格']
                        current_price_row_idx = (df_plan['buy_price']<=current_price) & (df_plan['sell_price']>=current_price)
                        if True in list(current_price_row_idx):
                            current_price_row = df_plan[current_price_row_idx].index[0]
                            target_net_num = df_plan.loc[current_price_row, 'net_num']
                        else:
                            current_price_row = -1
                            target_net_num = 0
                        target_idxs = df_plan.loc[df_plan['net_num']==current_net_num].index
                        if len(target_idxs) > 0: 
                            current_net_row = target_idxs[0]
                            if current_net_num < target_net_num: #need buy more
                                buy_postions = sum(df_plan.loc[current_net_row+1:current_price_row-1, 'buy_position'])
                                if buy_postions > 0:
                                    momentum_price = current_price
                                    momentum_share = round(buy_postions/current_price, -2)
                                    momentum_value = momentum_price*momentum_share
                            elif current_net_num > target_net_num: #need sell more
                                if target_net_num != 0:
                                    sell_shares = sum(df_plan.loc[current_price_row+1:current_net_row, 'sell_share'])
                                else:
                                    sell_shares = sum(df_plan.loc[0:current_net_row, 'sell_share'])
                                if sell_shares < 0:
                                    momentum_price = current_price
                                    momentum_share = sell_shares
                data.append([name, code, date, momentum_share, momentum_price, momentum_value, sheetName])
        task  = pd.DataFrame(data=data, columns=header)
        return task

def getTargetArea(sht, startCol, endCol=[], startRow=0, endRow=0):
    if len(endCol) > 0:
        header = sht.range(startCol+str(1), endCol+str(1)).value
    else:
        header = sht.range(startCol+str(1)).value
    numLastRow_Region = sht.range(startCol+str(1)).current_region.last_cell.row
    for i in range(numLastRow_Region+1):
        cellValue = sht.range(startCol+str(i+1)).value
        if cellValue == None or cellValue == "None" or cellValue == "nan":
            numLastRow = i
            break
    
    if startRow == 0:
        startRow = 2
    elif startRow < 0:
        startRow += numLastRow + 1
    
    if endRow == 0:
        endRow = numLastRow
    elif endRow < 0:
        endRow += numLastRow + 1
      
    if len(endCol) > 0:    
        data =  sht.range(startCol+str(startRow), endCol+str(endRow)).value
        if numLastRow == 2:
            data = [data]
        return pd.DataFrame(data=data, columns=header)
    else:
        data =  sht.range(startCol+str(2), startCol+str(endRow)).value
        return pd.Series(data=data, name=header).to_frame()

def updateRelativeMomentumWrapper(input_instance):
    return TradeSubject.updateRelativeMomentumV2(*input_instance)


class Trade:
    def __init__(self, xlsxName):
        self.xlsxName = xlsxName
        if xlsxName == '本金账本.xlsx':
            self.sheetDFs = pd.read_excel('指数和基金.xlsx', None)
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
        stockObjs = [TradeSubject(self, stockNumberStr, isStock=True) for stockNumberStr in self.stockNumberStrs]
        fundObjs = [TradeSubject(self, fundNumberStr, isStock=False) for fundNumberStr in self.fundNumberStrs]
        self.tradeObjs = stockObjs + fundObjs
        self.tradedsheetNames = [ obj.sheetName for obj in self.tradeObjs]
        self.objMap = dict(zip(self.tradedsheetNames, self.tradeObjs))
#        self.sheetsToDFs()
    
    def reConnectExcelEngines(self):
        for obj in self.tradeObjs:
            obj.reConnectExcelEngine(self)
    
    def reopen(self):
        self.wb = xw.Book(self.xlsxName)
    
    def sheetsToDFs(self):
        for obj in self.tradeObjs:
            obj.sheetToDF()
            
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

    def resetSheets(self):
        for obj in self.tradeObjs:
            TradeSubject.resetSheet(obj, self.pool)
 
    def updateSheets(self, tradeObjs=[]):
        if len(tradeObjs) == 0:
            tradeObjs = self.tradeObjs
#        for obj in tradeObjs:
#            TradeSubject.updateSheet(obj)
        self.pool.map(TradeSubject.updateSheet, tradeObjs)
        self.writeUpdatedSheets(tradeObjs)
        
    def updateSheetsV2(self, tradeObjs=[]):
        if len(tradeObjs) == 0:
            tradeObjs = self.tradeObjs
#        for obj in tradeObjs:
#            TradeSubject.updateSheet(obj)
        for subObj in tradeObjs:
            thatSubDF = self.sheetDFs[subObj.sheetName]
            thatTime = numberToDateTime(float(thatSubDF[subObj.DateStr].iloc[-1]))
            startDate = dateStrToDateTime(subObj.lastUpdateDateStr)
            if startDate < thatTime:
                sht_new_df = thatSubDF.copy()
                sht_new_df.loc[:,subObj.DateStr] = sht_new_df.loc[:,subObj.DateStr].apply(numberToDateStr)
                sht_appended = sht_new_df[sht_new_df[subObj.DateStr].map(dateStrToDateTime) > startDate].copy()
                sht_appended[subObj.DateStr] = sht_appended[subObj.DateStr].apply(numberToDateStr)
                subObj.newContents = sht_appended.values.tolist()
                if len(subObj.newContents) > 0:
                    subObj.hasNewContent = True
                else:
                    subObj.hasNewContent = False
        self.writeUpdatedSheets(tradeObjs)
#        thatObj.close()
   
    def writeUpdatedSheets(self, tradeObjs=[]):
        if len(tradeObjs) == 0:
            tradeObjs = self.tradeObjs
        for obj in tradeObjs:
            obj.writeUpdatedSheet()
        
    def updateMomentums(self):
        for obj in self.tradeObjs:
            obj.preCondition()
        momentum_results = self.pool.map(TradeSubject.updateMomentum, self.tradeObjs)
#        self.momentum_results = []
        for i in range(len(self.tradeObjs)):
#            self.momentum_results.append(TradeSubject.updateMomentum(self.tradeObjs[i]))
            self.shf_config.range(self.momentumMaps[i]).value = momentum_results[i]
            
    def updateRelativeMomentums(self):
        for obj in self.tradeObjs:
            obj.preCondition()
        baseObj = self.tradeObjs[0]
        inputs = [[subObj, baseObj] for subObj in self.tradeObjs]
        momentum_results = self.pool.map(updateRelativeMomentumWrapper, inputs)
#        momentum_results = []
        shf = self.wb.sheets['趋势']
        for i in range(len(self.tradeObjs)):
#            momentum_results.append(TradeSubject.updateRelativeMomentumV2(self.tradeObjs[i], baseObj))
            shf.range('V'+str(i+2)).value = momentum_results[i]      
            
    def updateStockSheetLive(self):
        sht_stockInfo = self.wb.sheets['股票查询']
    #    lastUpdateTime = sht_fundInfo.range('AJ2').value
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
        targetDate = getLastTradedTime()+relativedelta(days=1)
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
             
    
    def updateFor28Trade(self):
        targets = ['S000016', 'S399300', 'S000905', 'S399006']
        targetObjs = [self.objMap[t] for t in targets]
        self.updateSheetsV2(targetObjs)
        sht = targetObjs[0].sht
        numLastRow = sht.range('A1').current_region.last_cell.row
        lastUpdateDateStr = numberToStr(sht.range(numLastRow, 1).value)
        return lastUpdateDateStr
    
    def dfFor28Trade(self, nDays):
        targets = ['S000016', 'S399300', 'S000905', 'S399006']
        header  = ['Date', 'SZ50', 'HS300', 'ZZ500','CYB']
        latestDates = []
        for t in targets:
            shf_temp        = self.wb.sheets[t]
            numLastRow_temp = shf_temp.range('A1').current_region.last_cell.row
            latestDate_temp = dateStrToDateTime(numberToDateStr(shf_temp.range('A'+str(numLastRow_temp)).value))
            latestDates.append(latestDate_temp)
        numDiffDays = [(t - latestDates[0]).days for t in latestDates]
        sameDate = [d == 0 for d in numDiffDays]
        if all(sameDate):
            date = shf_temp.range('A'+str(numLastRow_temp-nDays+1),'A'+str(numLastRow_temp)).value
            date = [numberToDateStr(d) for d in date]
            data = []
            for t in targets:
                shf_temp        = self.wb.sheets[t]
                numLastRow_temp = shf_temp.range('A1').current_region.last_cell.row
                data_temp = shf_temp.range('E'+str(numLastRow_temp-nDays+1),'E'+str(numLastRow_temp)).value
                data.append(data_temp)
            data = [date] + data
            return pd.DataFrame(zip(*data), columns = header)
        else:
            raise Exception("Latest dates for 28 trading are not the same")                 
    
    
    def updateZZQZ(self):
        subObj = self.objMap['S000985']
        startDate = dateStrToDateTime(subObj.lastUpdateDateStr)  
        todayDate = getTodayDate()
        if startDate < todayDate:
    #        sht_new_df = getStockHistoryV2(buildStockNumberStr('000985'), startDate, todayDate)
    #        try
            sht_new_df = getZZQZLatestMonth()
            if len(sht_new_df)>0:
                sht_appended = sht_new_df[sht_new_df[subObj.DateStr].map(dateStrToDateTime) > startDate].copy()
                sht_appended[subObj.DateStr] = sht_appended[subObj.DateStr].apply(numberToDateStr)
                subObj.newContents = sht_appended.values.tolist()
                if len(subObj.newContents) > 0:
                    subObj.hasNewContent = True
                else:
                    subObj.hasNewContent = False
            else:
                subObj.hasNewContent = False
#        self.objMap['S000985'] = subObj
    
    def momentumDayKTrade(self, times=10):
        self.updateSheetsV2([self.objMap['S000985']])
        sht = self.wb.sheets['日线交易']
        df = getTargetArea(self.objMap['S000985'].sht, 'A', 'G', -1, -3*24)
        df = BBI(df)
        df = MAs(df, [5, 10 ,12, 20])
        df = AMAs(df)
        
        latestDateStr = numberToDateStr(float(df['日期'].iloc[-1]))
#        latestTClose = df['收盘价'].iloc[-1]
        latestStockDateStr = dateTimeToDateStr(self.wb.sheets['股票查询'].range('AK2').value)
        if latestDateStr != latestStockDateStr:# try another time to update S000985
            times -= 1
            if times >= 0:
                print('{} S000985 is not updated, waiting to try again'.format(getNowTimeStr()))
                time.sleep(60*2)
                callBatchMethod(self, 'updateZZQZ')
                ZZQZupdated = self.objMap['S000985'].hasNewContent
                if ZZQZupdated:
                    print("Has new content")
                    self.writeUpdatedSheets()
                self.momentumDayKTrade(times)
            else:
                raise('S000985 is not updated after 10 times')
            return
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
#                symbol_name = daysInName[0]
#                if 'BBI' in daysInName:
#                    df3 = BBI(df)
#                    first_value = latestTClose
#                    second_value = df3[symbol_name].iloc[-1]
#                elif 'AMA' in daysInName:
#                    df3 = AMAs(df)
#                    first_value  = df3['AMA5'].iloc[-1]
#                    second_value = df3['AMA10'].iloc[-1]
#                else:
#                    checkingDay_temp = int(daysInName[0].split('MA')[1])
#                    df3 = MAs(df, [checkingDay_temp])
#                    first_value = latestTClose
#                    second_value = df3[symbol_name].iloc[-1]
                sht.range(startCol1+str(startRow+i)).value = first_value
                sht.range(startCol2+str(startRow+i)).value = second_value
                sht.range(startCol3+str(startRow+i)).value = time.strftime("%d/%m/%Y, %H:%M:%S")
        


    def momentum28Trade(self):
        targets = ['SZ50', 'HS300', 'ZZ500','CYB']
        numTarget = len(targets)
        latestDateStr = self.updateFor28Trade()
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
            targetObjs = [ self.objMap[sheetName] for sheetName in sheetNames]
            self.updateSheetsV2(targetObjs)#update before calculation
            for obj in targetObjs:
                temp_sht = obj.sht
                temp_heads = temp_sht.range('A1', 'F1').value
                if '日期' in  temp_heads:
                    temp_TCloseStr = '收盘价'
                else:
                    temp_TCloseStr = '累计净值'
                temp_TClose_ColumnStr = getColumnStr(temp_heads.index(temp_TCloseStr)+1)
                numLastRow_temp = temp_sht.range('A1').current_region.last_cell.row
                temp_LatestTClose = temp_sht.range(indCell(temp_TClose_ColumnStr, numLastRow_temp)).value
                temp_offsetTClose = temp_sht.range(indCell(temp_TClose_ColumnStr, numLastRow_temp-offSetDays)).value
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
            Helper.sendEmail('今日汇总', msg, 'chenjiayi_344@hotmail.com', self.imgPath)
        else:
            Helper.sendEmail('今日汇总', msg, 'chenjiayi_344@hotmail.com')
    
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
        sysObj.imgs.append(dfToImg(df_lastTasks_traded))
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
#        sysObj = TradeSystem('本金账本.xlsx')
        sysObj.batchMethods = {}
        sysObj.batchMethods['updateSheetsV2'] = sysObj.updateSheetsV2
        sysObj.batchMethods['updateMomentums'] = sysObj.updateMomentums
        sysObj.batchMethods['updateRelativeMomentums'] = sysObj.updateRelativeMomentums
        sysObj.batchMethods['calculateForMomentumShares'] = sysObj.calculateForMomentumShares
        sysObj.batchMethods['updateStockSheetLive'] = sysObj.updateStockSheetLive
        sysObj.batchMethods['updateFundSheetLive'] = sysObj.updateFundSheetLive
        sysObj.batchMethods['updateGoldPrice'] = sysObj.updateGoldPrice
        sysObj.batchMethods['updateZZQZ'] = sysObj.updateZZQZ
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
                ExcelHelper.writeTradedTasks(sysObj, df_lastTasks_traded)
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
            callBatchMethod(sysObj, 'updateSheetsV2')
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
                ExcelHelper.writeMomentTasks(sysObj, tasks_valided)
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
#    sysObj.msg.append('Ordered task:')
#    sysObj.msg.append(msg)
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
        Helper.sendEmail('Alert', msg, 'chenjiayi_344@hotmail.com')
        return -1
    else:
#    moneys = [requiredCredit, availableCredit]
        if len(orderedTable) > 0:
            exitCode, sysObj = SmartQ_Python(sysObj, ordersTable, availableCredit)
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
    if datetime.now().timetuple().tm_wday == 6:
        resetZZQZ()
    sysObj = TradeSystem('本金账本.xlsx')
    updatingOnly(sysObj, weekday, afterEarlySummary) 
    time.sleep(5)
    if weekday != 5:
        subprocess.run(["D:\\Dropbox\\For daily life\\Investment\\RunHuatai.exe"])  
        exitCode, sysObj = orderingOnly(sysObj)
    else:
        exitCode = 0
        sysObj.save()
        print("Saving done 00\n")
        sysObj.close()
    sysObj.sendSummary()
    return exitCode

if __name__ == '__main__':
#    exitCode = runRoutine()
    sysObj = TradeSystem('本金账本.xlsx')
    updatingOnly(sysObj, weekday=1)
    sysObj.sendSummary()
#    sysObj2 = TradeSystem('指数和基金.xlsx')
#    app = xw.apps.active
    
#    sysObj.initialSubjects()
#    sysObj.updateSheetsV2()
#    sysObj.momentumDayKTrade()
#    sysObj.initialSubjects()
#    sysObj.updateFundSheetLive()
#    sysObj.updateStockSheetLive()
#    sysObj.momentumDayKTrade()
#    sysObj.initialSubjects()
#    sysObj.momentumDayKTrade()
#    sysObj.momentumDayKTrade()
#    runRoutine()
#    sysObj.initialSubjects()
#    sysObj.updateFundSheetLive()
#    hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
#    if hasTraded: 
#        ExcelHelper.writeTradedTasks(sysObj, df_lastTasks_traded)
#    sysObj.updateStockSheetLive()
#    orderingOnly(sysObj)
#    updatingOnly(sysObj)
#    runRoutine(1)
    pass
#    sysObj.initialSubjects()
#    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
#    ordersTable = sysObj.buildTargetValueTasks(ordersTable)
#    ordersTable = sysObj.buildMomentumTasks(ordersTable)
#    ordersTable = sysObj.removeInvalidTasks(ordersTable)
#    orderedTable = sysObj.getOrderedTasks(ordersTable)
#    targetCols = orderedTable.columns[:9].to_list()
#    orderedTable = orderedTable.loc[:, targetCols]
#    print('These tasks are prapared for order:')
#    print(orderedTable.to_string())
#    orderingOnly(sysObj)
#    runRoutine()
#    df_lastTasks = sysObj.getLastTasks()
#    exitCode = updatingOnly(sysObj)
#    exitCode = orderingOnly(sysObj)
#    sysObj.initialSubjects()
#    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
#    ordersTable = sysObj.buildTargetValueTasks(ordersTable)
#    ordersTable = sysObj.buildMomentumTasks(ordersTable)
#    ordersTable = sysObj.removeInvalidTasks(ordersTable)
#    sysObj.initialSubjects()
#    sysObj.momentum28Trade()
#    hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
#    updatingOnly(sysObj)
#    sysObj.updateStockSheetLive()
#    sysObj.updateFundSheetLive()
#    hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
#    subprocess.run(["D:\\Dropbox\\For daily life\\Investment\\RunHuatai.exe"])  
#    exitCode = orderingOnly(sysObj)
#    sysObj.calculate()
#    exitCode, tasks_valided = sysObj.calculateForMomentumShares()
    
#    ExcelHelper.writeTradedTasks(sysObj, df_lastTasks_traded)
#    print('These traded tasks are written:')
#    print(df_lastTasks_traded.to_string())
#    ExcelHelper.writeMomentTasks(sysObj, tasks_valided)
#    sysObj.calculate()
    
#    sysObj.updateStockSheetLive()
#    sysObj.calculate()
#    updatingOnly(sysObj)
#    sysObj.initialSubjects()
#    sysObj.momentumIndustry()
#    updatingOnly(sysObj)
    
#    sysObj.updateMomentums()
#    sysObj.updateRelativeMomentums()
#    baseObj = sysObj.tradeObjs[0]
#    targetObj = sysObj.tradeObjs[-1]
#    baseObj.preCondition()
#    targetObj.preCondition()
#    momentumResult = TradeSubject.updateRelativeMomentumV2(targetObj, baseObj)
    
#    sendSummary(sysObj, df_lastTasks_traded)
#    exitCode, tasks_valided = sysObj.calculateForMomentumShares()
#    ExcelHelper.excelWriter()
#    sysObj.initialSubjects()
    
    
#      sysObj.updateSheets()
#      sysObj.updateMomentums()
#    sysObj.updateRelativeMomentums()
#      sysObj.momentum28Trade()
#      updatingOnly(sysObj)
#         # ordersTable = sysObj.buildMomentumTasks()
      
#    hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
#    main_sheetNames = sysObj.sheetNames
#    hasTraded, df_lastTasks_traded= sysObj.compareWithLastTasks()
#     # sysObj.initialSubjects()
# #    sysObj.momentum28Trade()
#    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
#    ordersTable = sysObj.buildTargetValueTasks(ordersTable)
#    ordersTable = sysObj.buildMomentumTasks(ordersTable)
#    ordersTable_new = ordersTable[ordersTable['TradeCode'] != -1]

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
