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
import sys
sys.path.append('D:\\LinuxWorkFolder\\TUD\\Python\\Library')
from addLibraries import Helper
from TradingSystemV3 import runRoutine
from TradingSystemV3 import extractBetween
from TradingSystemV3 import dateTimeToDateStr
from TradingSystemV3 import timeStrToDateTime
#from TradingSystemV3 import datetimeToTimeStr
from TradingSystemV3 import getNowTimeStr
from TradingSystemV3 import getTodayDateStr
from TradingSystemV3 import TradeSystem, callBatchMethod
from UpdatingRiskRebalance import runRoutine as checkRiskRebalance
#from TradingSystemV3 import timeStrToDateStr
import datetime
from dateutil.relativedelta import relativedelta
import time
import multiprocessing as mp


tradingHour = 20
fridaySummaryHour = 12
atMainLoop = mp.Value('i', 0)

def getReportTimeStr():
    return  datetime.datetime.strftime(datetime.datetime.now(), "%d/%m/%Y %H:%M:%S")

def datetimeToDict(date_time):
    timeStruc = date_time.timetuple()
    out = {}
    out['Year'] = timeStruc.tm_year
    out['Month'] = timeStruc.tm_mon
    out['Day'] = timeStruc.tm_mday
    out['Weekday'] = timeStruc.tm_wday
    out['Hour'] = timeStruc.tm_hour
    out['Min'] = timeStruc.tm_min
    out['Second'] = timeStruc.tm_sec
    return out
        
def dictToDateTime(timeDict):
    list_a = [timeDict['Year'], timeDict['Month'], timeDict['Day'], timeDict['Hour'], timeDict['Min'], timeDict['Second']]
    return datetime.datetime(*list_a)

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
 
def writeLastTradedTime():    
    with open('TradingSystemV4_log.txt', 'at+') as log:
        log.write('Success at {}\n'.format(getNowTimeStr()))  
        
def fixTargetTradingTime(targetTradeTime):
    targetTradeTimeDict = datetimeToDict(targetTradeTime)
    targetTradeTimeDict['Min'] = 00
    targetTradeTimeDict['Second'] = 00
    return dictToDateTime(targetTradeTimeDict)
    
def debugTradingTime(targetTradeTime):
    targetTradeTimeDict = datetimeToDict(targetTradeTime)

    targetTradeTimeDict['Min'] = 00
    targetTradeTimeDict['Second'] = 1
    return dictToDateTime(targetTradeTimeDict)

def getTargetTradingTime(lastTradedTime):
    lastTradedTimeWeekday = lastTradedTime.timetuple().tm_wday + 1
    if lastTradedTimeWeekday in [1, 2, 3, 7]:
        targetTradeTime = lastTradedTime + relativedelta(days=1)
        targetTradeTimeDic = datetimeToDict(targetTradeTime)
        targetTradeTimeDic['Hour'] = tradingHour
    elif lastTradedTimeWeekday == 4:
        targetTradeTime = lastTradedTime + relativedelta(days=1)
        targetTradeTimeDic = datetimeToDict(targetTradeTime)
        targetTradeTimeDic['Hour'] = fridaySummaryHour
    elif lastTradedTimeWeekday == 5 and lastTradedTime.hour < tradingHour:
        targetTradeTime = lastTradedTime
        targetTradeTimeDic = datetimeToDict(targetTradeTime)
        targetTradeTimeDic['Hour'] = tradingHour
    elif lastTradedTimeWeekday == 5 and lastTradedTime.hour >= tradingHour:
        targetTradeTime = lastTradedTime + relativedelta(days=2)
        targetTradeTimeDic = datetimeToDict(targetTradeTime)
        targetTradeTimeDic['Hour'] = tradingHour
    else:
        raise Exception("Not at target weekdays!")
    return fixTargetTradingTime(dictToDateTime(targetTradeTimeDic))

def sleep(seconds): #for KeyboardInterrupt 
    for i in range(seconds):
        time.sleep(1)

def keepIndexUpdated():
    print('Multithreading to keep index updated.')
    sysObj = TradeSystem('指数和基金.xlsx')
    sysObj.initialSubjects()
    sysObj.batchMethods = {}
    sysObj.batchMethods['updateSheets'] = sysObj.updateSheets
    sysObj.batchMethods['updateZZQZ'] = sysObj.updateZZQZ
    sysObj.close()
#    targetSheetNames = ['S000985']
    global atMainLoop
    while True:
        nowTime = datetime.datetime.now()
        nowTimeTuple = nowTime.timetuple()
#        if nowTimeTuple.tm_wday in list(range(5)):
        if  atMainLoop.value == 0 and nowTimeTuple.tm_hour >= 10 and nowTimeTuple.tm_hour < tradingHour:
            print("Start updating loop")
            hasNewContent = False
            callBatchMethod(sysObj, 'updateZZQZ')
            callBatchMethod(sysObj, 'updateSheets')
#                ZZQZupdated = sysObj.objMap['S000985'].hasNewContent
#                if ZZQZupdated:
#                    hasNewContent = 
#                    print("Has new content")
#                    sysObj.reopen()
#                    sysObj.reConnectExcelEngines()
#                    sysObj.writeUpdatedSheets()
#                    sysObj.calculate()
#                    sysObj.initialSubjects()
#                    sysObj.save()
#                    sysObj.close()
#                    print("New content is written")
#                callBatchMethod(sysObj, 'updateSheets')
            for targetSheetName in sysObj.tradedsheetNames:
#                    print("Starting {}".format(targetSheetName))
                subObj = sysObj.objMap[targetSheetName]
#                    TradeSubject.updateSheet(subObj)
                if not hasNewContent and subObj.hasNewContent:
                    hasNewContent = True
#                    print("Finishing to update {}".format(targetSheetName))
            if hasNewContent and atMainLoop.value == 0:
                print("Has new content")
                sysObj.reopen()
                sysObj.reConnectExcelEngines()
                sysObj.writeUpdatedSheets()
                sysObj.calculate()
                sysObj.initialSubjects()
                sysObj.save()
                sysObj.close()
                print("New content is written")
        else:
            print("Not updating during target hours")
#        else:
#            print("")
#            print("Not updating during target weekdays")
        print("")
        print("Sleeping for 10 mins")
        sleep(60*10)

def monitorTradeSystem():
    while True:
        lastTradedTime = getLastTradedTime()
        targetTradeTime = getTargetTradingTime(lastTradedTime) + relativedelta(minutes=30)
        nowTime = datetime.datetime.now()
        if nowTime > targetTradeTime:
            Helper.sendEmail('Alert', 'TradeSystem failed', 'chenjiayi_344@hotmail.com')
            sleep(60*30)
        else:
            sleep(60*10)

if __name__ == '__main__':
    needAssistance = False
    p1 = mp.Process(target=keepIndexUpdated)
    p1.start()
    p2 = mp.Process(target=monitorTradeSystem)
    p2.start()
    sleep(60)
    while not needAssistance: #loop 1: between each successful trading
        lastTradedTime = getLastTradedTime()
        targetTradeTime = getTargetTradingTime(lastTradedTime)
#        print("Target time is {}".format(targetTradeTime))
#        sleepCounter = 1
        while True: #loop 2 check time for trading
            nowTime = datetime.datetime.now()
            if nowTime > targetTradeTime:
                atMainLoop.value = 1
                print('\n')
                checkRiskRebalance()
                if targetTradeTime.timetuple().tm_wday != 4:
                    print("Starting daily routine\n")
                    exitCode = runRoutine()
                    print("Finishing daily routine\n")
                else:
                    if nowTime.hour < tradingHour:
                        print("Starting Friday's early summary routine\n")
                        exitCode = runRoutine(5, afterEarlySummary=False)#friday evening early summary
                        print("Finishing Friday's early summary routine\n")
                    else:
                        print("Starting Friday's after early summary routine\n")
                        exitCode = runRoutine(5, afterEarlySummary=True)#friday evening after early summary
                        print("Finishing Friday's after early summary routine\n")
                atMainLoop.value = 0
                if exitCode == 0:
                    writeLastTradedTime()
                    sleep(5)
                    break #break loop 2
                else:
                    needAssistance = True #break loop 1
                    print('Need assistance...\n')
                    break #break loop 2
            else:
#                print('During sleep 2...', flush=True, end='')
                # sys.stdout.write('\r')
                msg = "{} Target time is {} ".format(getReportTimeStr(), targetTradeTime)
                print("\r", msg, end="")
                # sys.stdout.write('During sleep 2... {}'.format(sleepCounter))
#                sleepCounter += 1
                # sys.stdout.flush()
                sleep(60)
  

 ##        sysObj.close()   

'''
    subprocess.run(["D:\\Dropbox\\For daily life\\Investment\\RunHuatai.exe"])    
    from SmartQ_Python import SmartQ_Python
    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
    SmartQ_Python(ordersTable)
    
'''