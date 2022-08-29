# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 21:49:53 2020

@author: Frank
"""

import pandas as pd
from datetime import datetime
from datetime import timedelta
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
import urllib
import urllib.error as error
from lxml import html
import re
import math
from socket import timeout
import numpy as np
from bs4 import BeautifulSoup
import itertools
from itertools import product as prod

from SelfTradingSystem.util.others import sleep
#from fake_useragent import UserAgent
#ua = UserAgent(verify_ssl=False, cache=False, use_cache_server=False)

from SelfTradingSystem.util.convert import (
    fundDateEleToDateStr, dateTimeToDateStr, 
    )
from SelfTradingSystem.util.extract import extractBetween
import json
import investpy

def getHTML(tempUrl, timeLimit=20):
    attempts = 5
    tempHTML = []
    while attempts > 0:
        try:
            tempHTML = urllib.request.urlopen(tempUrl, timeout=timeLimit)
            break
        except error.URLError as e:
            attempts -= 1
            sleep(5)
            # print("Error {} during obtaing HTML {} remain attempts {}".format(e.reason, tempUrl, attempts))
        except timeout:
            attempts -= 1
            sleep(5)
            # print("Time out during obtaing HTML{} remain attempts {}".format(tempUrl, attempts))
    return tempHTML    

def getHTMLFromSina(tempUrl, timeLimit=20):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'
    headers = {'User-Agent': user_agent,
               'Referer':"http://finance.sina.com.cn"}
    attempts = 5
    tempHTML = []
    while attempts > 0:
        try:
            request=urllib.request.Request(tempUrl,None,headers) 
            tempHTML = urllib.request.urlopen(request, timeout=timeLimit)
            # tempHTML = urllib.request.urlopen(tempUrl, timeout=timeLimit)
            break
        except error.URLError as e:
            attempts -= 1
            sleep(5)
            # print("Error {} during obtaing HTML {} remain attempts {}".format(e.reason, tempUrl, attempts))
        except timeout:
            attempts -= 1
            sleep(5)
            # print("Time out during obtaing HTML{} remain attempts {}".format(tempUrl, attempts))
    return tempHTML    

def getHTML2(tempUrl, timeLimit=20):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'
    # headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0',
    #          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    #          'Accept-Encoding': 'gzip, deflate, br',
    #          'Accept-Language':'en-US,en;q=0.5',
    #          'Connection':	'keep-alive',
    #          'Host':	'www.csindex.com.cn',
    #          'Sec-Fetch-Dest':	'document',
    #          'Sec-Fetch-Mode':	'navigate',
    #          'Sec-Fetch-Site':	'n',
    #          } 
    headers = {'User-Agent': user_agent}
    attempts = 5
    tempHTML = []
    while attempts > 0:
        try:
            request=urllib.request.Request(tempUrl,None,headers) 
            tempHTML = urllib.request.urlopen(request, timeout=timeLimit)
            # tempHTML = urllib.request.urlopen(tempUrl, timeout=timeLimit)
            break
        except error.URLError as e:
            attempts -= 1
            sleep(5)
            # print("Error {} during obtaing HTML {} remain attempts {}".format(e.reason, tempUrl, attempts))
        except timeout:
            attempts -= 1
            sleep(5)
            # print("Time out during obtaing HTML{} remain attempts {}".format(tempUrl, attempts))
    return tempHTML      
            
def getSeasonNum(date):
    return (date.month-1)//3 + 1

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

def splitStockStrFromTencent(stockStr):
    indicesOfDoubleQuotation = getIndexOfChar(stockStr, '"')
    message = "The stock string is not double quotated: {}".format(stockStr)
    assert len(indicesOfDoubleQuotation) == 2, message
    stockStr = stockStr[indicesOfDoubleQuotation[0]+1:indicesOfDoubleQuotation[1]]
    result = stockStr.split('~')
    cols = [1,5,4,3,33,34,9,19,36,37,10,9,12,11,14,13,16,15,18,17,20,19,22,21,24,23,26,25,28,27]
    amount_cols =  list(range(10, 28+1, 2))
    final = [result[i] for i in cols]
    final[9] = str(float(final[9])*10000)
    for c in amount_cols: final[c] = str(float(final[c])*100)
    date = datetime.strptime(result[30], '%Y%m%d%H%M%S')
    final.append(date.strftime('%d/%m/%Y'))
    final.append(date.strftime('%H:%M:%S'))
    return final

def splitFundStr(fundStr):
    indicesOfDoubleQuotation = getIndexOfChar(fundStr, '"')
    message = "The stock string is not double quotated: {}".format(fundStr)
    assert len(indicesOfDoubleQuotation) == 2, message
    indicesOfComma           = getIndexOfChar(fundStr, ',')
    numComma = len(indicesOfComma)
    result = []
    for i in range(numComma):
        if i==0:
            start = indicesOfDoubleQuotation[0]+1
            end   = indicesOfComma[0]
        else:
            start = indicesOfComma[i - 1] + 1
            end   = indicesOfComma[i]
        result.append(fundStr[start:end])
        if i==numComma-1:
            start = indicesOfComma[i] + 1
            end   = indicesOfDoubleQuotation[1]
            result.append(fundStr[start:end])
    return result

def splitFundStrFromTencent(fundStr):
    indicesOfDoubleQuotation = getIndexOfChar(fundStr, '"')
    message = "The stock string is not double quotated: {}".format(fundStr)
    assert len(indicesOfDoubleQuotation) == 2, message
    result = fundStr.split('~')
    cols = [8, 5, 6, 7]
    final = [result[c] for c in cols]
    try:
        final[0] = datetime.strptime(final[0],'%Y%m%d')
    except:
        final[0] = datetime.strptime(final[0],'%Y-%m-%d')
        
    final[3] = str(round(float(final[3]), 2)) + '%'
    return final

stockDict = {};
stockDict['000985'] = 'CSI All Share TR'

def getStockFromInvest(stockNumberStr, startDate=datetime.strptime('01/01/2010','%d/%m/%Y') , endDate=datetime.today()):
    df = []
    if stockNumberStr in stockDict:
        df = investpy.indices.get_index_historical_data(index=stockDict[stockNumberStr],
                                        country='china',
                                        from_date=startDate.strftime("%d/%m/%Y"),
                                        to_date=endDate.strftime("%d/%m/%Y"))
        df['Date'] = df.index
        df['Date'] = df['Date'].apply(dateTimeToDateStr)
        df = df.reset_index(drop=True)
        df = df.loc[:, ['Date', 'Open', 'High', 'Low', 'Close']]
        df.columns = ['日期', '开盘价', '最高价', '最低价', '收盘价']
    return df

def getStock(stock):
    tempUrl = "http://hq.sinajs.cn/list=" + stock
    tempHTML = getHTMLFromSina(tempUrl)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("gbk")
    stockStr_list = splitStockStr(tempStr)
    if len(stockStr_list) < 33:
        stockStr_list.append(''*(32-len(stockStr_list)))
    return stockStr_list

def getStocksBatch(stocks):
    if len(stocks) > 30:
        results = getStocksBatch(stocks[:29]) + getStocksBatch(stocks[29:])
    else:
        results = getStocks(stocks)
    return results
    
def getStocks(stocks):
    sleep(5)
    longStock = ",".join(stocks)
    tempUrl = "http://hq.sinajs.cn/list=" + longStock
    tempHTML = getHTMLFromSina(tempUrl, timeLimit=40)
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

def getStockName(stock):
    stockNumberStr = ''.join(re.findall('[\d]', str(stock)))
    stockNumber = int(stockNumberStr)
    if stockNumber > 500000:
        stockNumberStr = 'sh'+buildIndexNumberStr(stockNumber)
    else:
        stockNumberStr = 'sz'+buildIndexNumberStr(stockNumber)
    stockList = getStock(stockNumberStr)
    return stockList[0]
 
def getStockFromTencent(stock):
    tempUrl = "https://qt.gtimg.cn/q=" + stock
    tempHTML = getHTML(tempUrl)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("gbk")
    stockStr_list = splitStockStrFromTencent(tempStr)
    if len(stockStr_list) < 33:
        stockStr_list.append(''*(32-len(stockStr_list)))
    return stockStr_list

def getStocksFromTencent(stocks):
    stockStr=','.join(stocks)
    tempUrl = "https://qt.gtimg.cn/q=" + stockStr
    tempHTML = getHTML(tempUrl)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("gbk")
    tempStrsList = tempStr.split('\n')
    tempStrsList = [tempStr for tempStr in tempStrsList if len(tempStr) > 0]
    stockStrs_list= []
    for tempStr in tempStrsList:
        stockStr_list = splitStockStrFromTencent(tempStr)
        if len(stockStr_list) < 33:
            stockStr_list.append(''*(32-len(stockStr_list)))
        stockStrs_list.append(stockStr_list)
    return stockStrs_list

   
def getStocksBatchFromTencent(stocks):
    if len(stocks) > 30:
        results = getStocksBatchFromTencent(stocks[:29]) + getStocksBatchFromTencent(stocks[29:])
    else:
        results = getStocksFromTencent(stocks)
    return results

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
            
    for i in range(len(results)):
        results[i][0] = datetime.strptime(results[i][0], "%Y-%m-%d")
    return results

def getFundHistory(fundNumberStr,  rows=0, pool=[]):
    defaultDayPerPage = 45
    tempUrl = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={}&page=1&per=1'.format(fundNumberStr)
    tempHTML = getHTML(tempUrl)
    finalResults = []
    if type(tempHTML) is not list:
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


def getFundLatest(fundNumberStrs):
    funds = ['of'+fund for fund in fundNumberStrs]
    longFund = ",".join(funds)
    tempUrl = "http://hq.sinajs.cn/list=" + longFund
    tempHTML = getHTMLFromSina(tempUrl, timeLimit=40)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("gbk");
    tempStrsList = tempStr.split('\n')
    tempStrsList = [tempStr for tempStr in tempStrsList if len(tempStr) > 0]
    fundStrs_list= []
    for tempStr in tempStrsList:
        fundStr_list = splitFundStr(tempStr)
        date = datetime.strptime(fundStr_list[-1], "%Y-%m-%d")
        fundStr_list_final = [date, *fundStr_list[1:3],fundStr_list[4]+'%']
        fundStrs_list.append(fundStr_list_final)
    return fundStrs_list

def getFundLatestFromTencent(fundNumberStrs):
    funds = ['jj'+fund for fund in fundNumberStrs]    
    longFund = ",".join(funds)
    tempUrl = "https://qt.gtimg.cn/q=" + longFund
    tempHTML = getHTML(tempUrl, timeLimit=40)
    tempContent = tempHTML.read()
    tempStr = tempContent.decode("gbk");
    tempStrsList = tempStr.split('\n')
    tempStrsList = [tempStr for tempStr in tempStrsList if len(tempStr) > 0]
    fundStrs_list= []
    for tempStr in tempStrsList:
        fundStr_list = splitFundStrFromTencent(tempStr)
        fundStrs_list.append(fundStr_list)
    return fundStrs_list


def getFundLatestBatchFromTencent(fundNumberStrs):
    if len(fundNumberStrs) > 30:
        results = getFundLatestBatchFromTencent(fundNumberStrs[:29]) + getFundLatestBatchFromTencent(fundNumberStrs[29:])
    else:
        results = getFundLatestFromTencent(fundNumberStrs)
    return results

def getFundCreateDate(fundNumberStr):
    url = 'http://stock.finance.sina.com.cn/fundInfo/view/FundInfo_JJGK.php?symbol={}'
    tempUrl = url.format(fundNumberStr)
    tempHtml = getHTML(tempUrl)
    tempContent = tempHtml.read()
    soup = BeautifulSoup(tempContent, features="lxml")
    tds  = soup.findAll('td')
    td_index = [i for i in range(len(tds)) if '成立日期' in str(tds[i])]
    dateStr = str(tds[td_index[0]+1])
    substr1='<td><span class="s2 f005">'
    substr2='</span></td>'
    date = extractBetween(dateStr, substr1, substr2)[0]
    date = datetime.strptime(date, "%Y/%m/%d")
    return date


def getWholeFundHistoryFromSina(fundNumberStr, pool=[]):
    date = getFundCreateDate(fundNumberStr)
    datefrom = datetime.strftime(date, "%Y-%m-%d")
    dateto   = datetime.strftime(datetime.today(), "%Y-%m-%d")
    df = getFundHistoryFromSinaBetween(fundNumberStr, datefrom, dateto, pool)
    return df

def getFundHistoryFromSinaBetween(fundNumberStr='162411', 
                  datefrom='1999-01-01',dateto='2021-03-12',  pool=[]):
    _, numPages = getFundHistoryFromSina(fundNumberStr, datefrom, dateto, 1)
    inputs = [[fundNumberStr, datefrom, dateto, i+1] for i in range(numPages)]
    if type(pool) is not list:
        results = pool.map(getFundHistoryFromSinaWrapper, inputs)
    else:
        results = [getFundHistoryFromSinaWrapper(input) for input in inputs]
    results = [result for result in results if len(result)>0]
    if len(results) > 0:
        df = pd.concat(results)
        df = df.drop_duplicates(subset='净值日期')
        df = df.reset_index(drop=True)
        df['累计净值'] = df['累计净值'].apply(float)
        df['单位净值'] = df['单位净值'].apply(float)
        df['累计净值'] = df['累计净值'].replace(to_replace=0, method='bfill')
        df['单位净值'] = df['单位净值'].replace(to_replace=0, method='bfill')
        lambda_f = lambda x: dateTimeToDateStr(datetime.fromisoformat(x))
        df['净值日期'] = df['净值日期'].apply(lambda_f)
        columns = [ '日增长率', '申购状态', '赎回状态', '分红送配']  
        df[columns] = ''
        values_after  = np.array(df['累计净值'].iloc[:-1])
        values_before = np.array(df['累计净值'].iloc[1:])
        percents = list(values_after / values_before -1)
        percents.append(0)
        df['日增长率'] = percents
        df['日增长率'] = df['日增长率'].apply(lambda x: round(x,4))
        df = df[::-1].reset_index(drop=True)
        return df
    else:
        return []

def getFundHistoryFromSinaWrapper(input):
    df, numPages = getFundHistoryFromSina(*input)
    return df

def getFundHistoryFromSina(fundNumberStr='162411', datefrom='1999-01-01',
                           dateto='2021-03-12', numPage=1):
    url = ('http://stock.finance.sina.com.cn/fundInfo/api/openapi.php',
           '/CaihuiFundInfoService.getNav?',
           'symbol={}&datefrom={}&dateto={}&page={}')
    tempUrl = ''.join(url).format(fundNumberStr, datefrom,dateto, numPage)
    tempHtml = getHTML(tempUrl)
    df = []
    numPages = 0
    columns = ['净值日期', '单位净值', '累计净值']
    if type(tempHtml) is not list:
        tempContent = tempHtml.read()
        tempStr = tempContent.decode("UTF-8")
        tempDict = json.loads(tempStr)
        dataDict = tempDict['result']['data']['data']
        numPages = math.ceil((int(tempDict['result']['data']['total_num'])/20))
        data = []
        for row in dataDict:
            data.append(list(row.values()))
        df = pd.DataFrame(data=data, columns=columns)
        pass
    return df, numPages

def getStockHistroyFrom163(stockNumberStr, thisYear=1990, thisSeason=1):
    if type(stockNumberStr) is list:
        temp = stockNumberStr
        stockNumberStr = temp[0]
        thisYear       = temp[1]
        thisSeason     = temp[2]
    tempUrl = 'http://quotes.money.163.com/trade/lsjysj_zhishu_{}.html?year={}&season={}'.format(stockNumberStr, thisYear, thisSeason);
    tempHTML = getHTML(tempUrl)
    df = []
    try:
        if type(tempHTML) is not list:
            tempContent = tempHTML.read()
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
            df = pd.DataFrame(results, columns=headers)
    except:
        pass
    return df
    
def getZZQZLatestMonth():
    url = 'http://www.csindex.com.cn/zh-CN/indices/index-detail/000985'
    tempHTML = getHTML2(url, timeLimit=40)
    df = []
    if type(tempHTML) is not list:
        tempContent = tempHTML.read()
        tempStr = tempContent.decode("utf-8")
        rawStr = extractBetween(tempStr, 'var data = [];', 'var hqt =')
        rawStr = rawStr[0]
        rawList = rawStr.split('\r\n\t\t\t\t\t\t')
        datas = []
        dates = []
        for raw in rawList[1:]:
            if 'dates' in raw:
                dateStr = extractBetween(raw, ' "', '";')[0]
                date = dateTimeToDateStr(datetime.strptime(dateStr, "%Y-%m-%d"))
                dates.append(date)
            if 'data' in raw:
                dataStr = extractBetween(raw, ' "', '";')[1]
                data = float(dataStr)
                datas.append(data)
        columns = ['日期','股票代码','名称','开盘价','最高价','最低价','收盘价','涨跌额','涨跌幅']
        df = pd.DataFrame(np.zeros([len(dates), len(columns)], dtype=float), columns=columns)
        df['日期'] =   dates  
        df['收盘价'] =  datas
    return df


def getStockHistroyFromCSINDEX(stockNumberStr, startDate=datetime.strptime('19990101', "%Y%m%d"), endDate=datetime.today()):
    tempUrl = 'https://www.csindex.com.cn/csindex-home/perf/index-perf?indexCode={}&startDate={}&endDate={}'.format(stockNumberStr, dateTimeToDateStr(startDate), dateTimeToDateStr(endDate))
    tempHtml = getHTML2(tempUrl, timeLimit=40)
    tempContent = tempHtml.read()
    tempStr = tempContent.decode("utf-8")
    tempDict = json.loads(tempStr)
    columns = ['日期','股票代码','名称','开盘价','最高价','最低价','收盘价',
               '涨跌额','涨跌幅', '成交量(股)', '涨跌幅(%)', '成交金额(元)']
    data = []
    columns_maps = [0, 1, 3, 6, 7, 8, 9, 10, 11, 12, 11, 13]
    if 'data' not in tempDict:
        return []
    else:
        tempList = tempDict['data']
        if len(tempList) == 0:
            return []
        for i in range(len(tempList)):
            row = []
            row_dict = tempList[i]
            row_keys = list(row_dict.keys())
            for j in columns_maps:
                value = row_dict[row_keys[j]]
                if j == 12:
                    value *= 1E6
                if j == 13:
                    value *= 1E8
                row.append(value)
            data.append(row)
        df = pd.DataFrame(data=data, columns=columns)
        return df
        

def getStockHistoryV2(stockNumberStr, startDate=datetime.strptime('19990101', "%Y%m%d"), endDate=datetime.today()):
    tempUrl = 'http://quotes.money.163.com/service/chddata.html?code={}&start={}&end={}&fields=TOPEN;HIGH;LOW;TCLOSE;CHG;PCHG'.format(buildStockNumberStr(stockNumberStr), dateTimeToDateStr(startDate), dateTimeToDateStr(endDate))
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
        season_end =  getSeasonNum(endDate)
        tasks = []
        for y in range(startYear, endYear+1):
            if y != endYear:
                task = list(zip([stockNumberStr]*4, [y]*4, seasons))
                task = [list(t) for t in task]
                tasks += task
            else:
                for s in range(season_end):
                    task = list(zip([stockNumberStr]*4, [y]*4, [s+1]))
                    task = [list(t) for t in task]
                    tasks += task
        if type(pool) is not list:
            results = pool.map(getStockHistroyFrom163, tasks)
        else:
            results = [getStockHistroyFrom163(task) for task in tasks]
            
        tempResults = []
        for result in results:
            if len(result) > 0:
                tempResults.append(result.iloc[::-1])
            else:
                return [] #avoid gap in stock history
        # results = [result.iloc[::-1] for result in results if len(result)>0]
        if len(tempResults) > 0:
            finalResults = pd.concat(tempResults)     
            finalResults.reset_index()
            return finalResults
        else:
            return []
            

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

def buildIndexNumberStr(indexNumberStr):
    indexNumber = int(indexNumberStr)
    numDigits = len(str(indexNumber))
    if numDigits == 6:
        indexNumberStr = str(indexNumber)
    elif numDigits < 6:
        extraLength = 6 - numDigits
        extraStr    = '0'*extraLength
        indexNumberStr = extraStr + str(indexNumber)
    return indexNumberStr

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


def getTClose(df):
    if '累计净值' in df.columns:
        TClose = df['累计净值'].values
    else:
        TClose = df['收盘价'].values
    TClose = [float(v) for v in TClose]
    return np.array(TClose)

def MAs(df, nums): #moving average 
    TClose = getTClose(df)        
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

if __name__ == '__main__':
    mainIndex = getStock('sh113044')
    # numDays = 10
    # stockStr = 'S000985'
    # todayDate = datetime.today()
    # stockNumberStr = stockStr[1:]
    # startDate = datetime.strptime('01/01/2010','%d/%m/%Y')
    # endDate=datetime.today()
    # df = getStockFromInvest(stockNumberStr, startDate, endDate)
   
    # dates = [todayDate+timedelta(days=-1*i) for i in range(numDays)]
    # dates = dates[::-1]
    # TClose = list(range(10))
    # df = pd.DataFrame(zip(dates, TClose), columns=['日期', '收盘价'])
    # MA3 = MA(TClose, 3)
    # stock = 'sh510300'
    # stockList = getStock(stock)
    # df = getZZQZLatestMonth()
    # sht_new_df = getFundHistoryFromSinaBetween(subobj.name, datefrom, dateto)
    pass
    
