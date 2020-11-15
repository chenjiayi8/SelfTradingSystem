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
    numDays = 10
    todayDate = datetime.today()
    dates = [todayDate+timedelta(days=-1*i) for i in range(numDays)]
    dates = dates[::-1]
    TClose = list(range(10))
    df = pd.DataFrame(zip(dates, TClose), columns=['日期', '收盘价'])
    MA3 = MA(TClose, 3)
