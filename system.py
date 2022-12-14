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
# import sys
# sys.path.append('D:\\LinuxWorkFolder\\TUD\\Python\\Library')
# from addLibraries import Helper

# def runRoutine():
#     pass

# def checkRiskRebalance():
#     pass

from SelfTradingSystem.core.trade import runRoutine
from SelfTradingSystem.io.database import Database
from SelfTradingSystem.core.riskRebalance import runRoutine as checkRiskRebalance
from SelfTradingSystem.util.convert import getNowTimeStr

from SelfTradingSystem.util.remindMe import sendEmailBatch as sendEmail
from SelfTradingSystem.util.others import (
    getLastTradedTime, sleep
    )

import queue
import time
import threading
import schedule
# from TradingSystemV3 import runRoutine
# from TradingSystemV3 import extractBetween
# from TradingSystemV3 import dateTimeToDateStr
# from TradingSystemV3 import timeStrToDateTime
#from TradingSystemV3 import datetimeToTimeStr
# from TradingSystemV3 import getNowTimeStr
# from TradingSystemV3 import getTodayDateStr
# from TradingSystemV3 import TradeSystem, callBatchMethod
# from UpdatingRiskRebalance import runRoutine as checkRiskRebalance
#from TradingSystemV3 import timeStrToDateStr
import datetime
from dateutil.relativedelta import relativedelta
# import multiprocessing as mp

logFile = 'log.txt'
tradingHour = 19
fridaySummaryHour = 12

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

 
def writeLastTradedTime(logFile):    
    with open(logFile, 'at+') as log:
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


def monitorTradeSystem():
    global logFile
    while True:
        lastTradedTime = getLastTradedTime(logFile)
        targetTradeTime = getTargetTradingTime(lastTradedTime) + relativedelta(minutes=30)
        nowTime = datetime.datetime.now()
        if nowTime > targetTradeTime:
            sendEmail('Alert from WinInWin', 'Trading system failed', 'chenjiayi_344@hotmail.com')
            sleep(60*30)
        else:
            sleep(60*10)

def keepIndexUpdated():
    db_path = 'Resources.db'
    sql = Database(db_path, tradingHour)
    sleep(5)
    sql.run()
    
    
    
import functools
def catch_exceptions(cancel_on_failure=False):
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                import traceback
                print(traceback.format_exc())
                if cancel_on_failure:
                    return schedule.CancelJob
        return wrapper
    return catch_exceptions_decorator

  
        
def monitorTask(logFile):
    lastTradedTime = getLastTradedTime(logFile)
    targetTradeTime = getTargetTradingTime(lastTradedTime) + relativedelta(minutes=30)
    nowTime = datetime.datetime.now()
    if nowTime > targetTradeTime:
        sendEmail('Alert from WinInWin', 'Trading system failed', 'chenjiayi_344@hotmail.com')
        sleep(60*30)
    
def keepUpdatedTask(sql):
    sql.keepUpdating()


@catch_exceptions(cancel_on_failure=True) 
def tradeTask(logFile):
    lastTradedTime = getLastTradedTime(logFile)
    targetTradeTime = getTargetTradingTime(lastTradedTime)
#        print("Target time is {}".format(targetTradeTime))
#        sleepCounter = 1
    # while True: #loop 2 check time for trading
    nowTime = datetime.datetime.now()
    if nowTime > targetTradeTime:
        print('\n')
        checkRiskRebalance()
        if targetTradeTime.timetuple().tm_wday != 4:
            print("Starting daily routine\n")
            exitCode = runRoutine()
            print("Finishing daily routine\n")
        else:
            if nowTime.hour < tradingHour:
                print("Starting Friday's early summary routine\n")
                exitCode = runRoutine(weekday=5, afterEarlySummary=False)#friday evening early summary
                print("Finishing Friday's early summary routine\n")
            else:
                print("Starting Friday's after early summary routine\n")
                exitCode = runRoutine(weekday=5, afterEarlySummary=True)#friday evening after early summary
                print("Finishing Friday's after early summary routine\n")
        if exitCode == 0:
            writeLastTradedTime(logFile)
            sleep(5)
            # break #break loop 2
        else:
            print('Need assistance...\n')
            # break #break loop 2
    else:
#                print('During sleep 2...', flush=True, end='')
        # sys.stdout.write('\r')
        msg = "{} Target time is {} ".format(getReportTimeStr(), targetTradeTime)
        print("\r", msg, end="")
            # sys.stdout.write('During sleep 2... {}'.format(sleepCounter))
#                sleepCounter += 1
            # sys.stdout.flush()
  
def updateMomentumTask(sql):
    print("Enter updateMomentumTask")
    sql.updateMomentums()
    print("Exit updateMomentumTask")

def printJobs():
    for job in schedule.jobs:
        print(job)

def worker_main():
    while 1:
        job_func, arg = jobqueue.get()
        job_func(arg)
        jobqueue.task_done()
        printJobs()


if __name__ == '__main__':
    # needAssistance = False
    db_path = 'Resources.db'
    sql = Database(db_path, tradingHour)
    jobqueue = queue.Queue()
    
    # schedule.every().day.at(str(tradingHour)+':00').do(jobqueue.put, (tradeTask, logFile))
    schedule.every(10).minutes.do(jobqueue.put, (tradeTask, logFile))
    schedule.every(10).minutes.do(jobqueue.put, (keepUpdatedTask, sql))
    schedule.every(30).minutes.do(jobqueue.put, (monitorTask, logFile))
    schedule.every().saturday.at('10:00').do(jobqueue.put, (updateMomentumTask, sql))
    printJobs()
    worker_thread = threading.Thread(target=worker_main)
    worker_thread.start()
    print("Main loop:")
    while 1:
        schedule.run_pending()
        time.sleep(1)

    
#     p1 = mp.Process(target=keepIndexUpdated)
#     p1.start()
#     p2 = mp.Process(target=monitorTradeSystem)
#     p2.start()
#     sleep(60)
#     while not needAssistance: #loop 1: between each successful trading
#         lastTradedTime = getLastTradedTime(logFile)
#         targetTradeTime = getTargetTradingTime(lastTradedTime)
# #        print("Target time is {}".format(targetTradeTime))
# #        sleepCounter = 1
#         while True: #loop 2 check time for trading
#             nowTime = datetime.datetime.now()
#             if nowTime > targetTradeTime:
#                 print('\n')
#                 checkRiskRebalance()
#                 if targetTradeTime.timetuple().tm_wday != 4:
#                     print("Starting daily routine\n")
#                     exitCode = runRoutine()
#                     print("Finishing daily routine\n")
#                 else:
#                     if nowTime.hour < tradingHour:
#                         print("Starting Friday's early summary routine\n")
#                         exitCode = runRoutine(weekday=5, afterEarlySummary=False)#friday evening early summary
#                         print("Finishing Friday's early summary routine\n")
#                     else:
#                         print("Starting Friday's after early summary routine\n")
#                         exitCode = runRoutine(weekday=5, afterEarlySummary=True)#friday evening after early summary
#                         print("Finishing Friday's after early summary routine\n")
#                 if exitCode == 0:
#                     writeLastTradedTime(logFile)
#                     sleep(5)
#                     break #break loop 2
#                 else:
#                     needAssistance = True #break loop 1
#                     print('Need assistance...\n')
#                     break #break loop 2
#             else:
# #                print('During sleep 2...', flush=True, end='')
#                 # sys.stdout.write('\r')
#                 msg = "{} Target time is {} ".format(getReportTimeStr(), targetTradeTime)
#                 print("\r", msg, end="")
#                 # sys.stdout.write('During sleep 2... {}'.format(sleepCounter))
# #                sleepCounter += 1
#                 # sys.stdout.flush()
#                 sleep(60)
  

 ##        sysObj.close()   

'''
    subprocess.run(["D:\\Dropbox\\For daily life\\Investment\\RunHuatai.exe"])    
    from SmartQ_Python import SmartQ_Python
    ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
    SmartQ_Python(ordersTable)
    
'''