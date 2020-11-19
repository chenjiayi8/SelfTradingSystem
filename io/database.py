# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 11:26:23 2020

@author: jiayichen
"""

from SelfTradingSystem.util.others import sleep
from SelfTradingSystem.util.convert import (
    dateStrToDateTime, numberToStr, getTodayDate
    )
from SelfTradingSystem.io.subject import Subject
import sqlite3
from sqlite3 import Error
import pandas as pd
import multiprocessing as mp
# try:
#     mp.set_start_method('spawn')
# except:
#     pass
import atexit
import datetime
import sys
import traceback
# from datetime import datetime
# from datetime import timedelta


def callBatchMethod(sql, methodStr):
    loopGuard = 3
    returnCode = 1
    while loopGuard > 0:
        try:
            print("Trying to {}\n".format(methodStr))
            sql.batchMethods[methodStr].__call__()
            print("{} done\n".format(methodStr))
            returnCode = 0
            break
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print ("Need assisstance for unexpected error:\n {}".format(sys.exc_info()))
            traceBackObj = sys.exc_info()[2]
            traceback.print_tb(traceBackObj)
            loopGuard -= 1
            print("Something is wrong during calling {}, try {} times\n".format(methodStr, loopGuard))
            returnCode = -1
    return returnCode

class Database():
    def __init__(self, db):
        self.db=db
        # self.pool = mp.get_context("spawn").Pool(8)
        # self.pool = mp.dummy.Pool(8)
        self.tradingHour = 20
        self.batchMethods = {}
        self.batchMethods['updateSubjects'] = self.updateSubjects
        self.batchMethods['validateZZQZ'] = self.validateZZQZ
        self.initialSubjects()
        self.running = mp.Value('i', 0)
        self.writing = mp.Value('i', 0)
        
        
    def initialSubjects(self):
        arg = ('SELECT name from sqlite_master where type= "table"')
        names = list(self.execute(arg))
        names = [name[0] for name in names]
        if 'Menu' in names:
            names.remove('Menu')
        self.subjectnames = names
        conn = self.create_connection_for_read()
        self.tradeObjs = []
        for name in names:
            if 'S' in name:
                isStock = True
            else:
                isStock = False
            subobj = Subject(name[1:], self, isStock=isStock)
            self.setLastUpdatedDate(subobj, conn)
            self.tradeObjs.append(subobj)
        self.objMap = dict(zip(names, self.tradeObjs))
        conn.close()
    
    def run(self):
        self.running.value = 1
        while self.running.value == 1:
            self.keepUpdating()
        
    def close(self):
        self.running.value = 0
        
    def create_connection_for_read(self):
        connection = None
        try:
            connection = sqlite3.connect(self.db, isolation_level=None, timeout=10)
            atexit.register(connection.close)
            # print("Connection to SQLite DB for read successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        return connection
    
    
    def create_connection_for_write(self):
        connection = None
        while self.writing.value == 1:
            print("Waiting for writing lock")
            sleep(10)
        try:
            connection = sqlite3.connect(self.db, isolation_level=None, timeout=10)
            connection.execute('pragma journal_model=wal')
            atexit.register(connection.close)
            print("Connection to SQLite DB for write successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        return connection

    
    def execute(self, arg):
        with self.create_connection_for_read() as conn:
            cursor = conn.cursor()
            cursor.execute(arg)
            results = cursor.fetchall()
            return results
    
    def createDB(self, xlsx_path, db_path):
        self.db = db_path
        conn = self.create_connection_for_read()
        dfs = pd.read_excel(xlsx_path, sheet_name = None)
        for table, df in dfs.items():
            df.to_sql(table, con=conn, if_exists='replace', index=False, method='multi')
        conn.close()
        
    def keepUpdating(self):
        print('Enter keepUpdating')
        todaydate = getTodayDate()
        nowTime = datetime.datetime.now()
        nowTimeTuple = nowTime.timetuple()
        if  nowTimeTuple.tm_wday < 6 and nowTimeTuple.tm_hour >= 10\
            and nowTimeTuple.tm_hour < self.tradingHour:
            print("Start updating loop")
            targetObjs = []
            for subjectname in self.subjectnames:
                subobj = self.objMap[subjectname]
                if subobj.lastUpdatedDate < todaydate:
                    targetObjs.append(subobj)
            # callBatchMethod(self, 'updateSubjects')
            print("Before updateSubjects")
            targetObjs = self.updateSubjects(targetObjs)
            print("updateSubjects done")
            tradedObjs = [subobj for subobj in targetObjs if subobj.hasNewContent]
            if len(tradedObjs) > 0:
                print("Has new content")
                self.writeSubjects(tradedObjs)
                print("writeSubjects done")
            callBatchMethod(self, 'validateZZQZ')
        else:
            print("Not updating during target hours")

        # self.validateZZQZ()
        sleep(60*10)
        print('Exit keepUpdating')
    
    def validateZZQZ(self):
        subjectname = 'S000985'
        subobj = self.objMap[subjectname]
        if subobj.validatedDate < subobj.lastUpdatedDate:
            df = subobj.getValidatedZZQZ()
            # print('ValidatedZZQZ:')
            # print(df)
            df_lastRow = self.getLastRows(subjectname, 20)
            conn = self.create_connection_for_write()
            self.writing.value == 1
            cur = conn.cursor()
            for i in range(len(df_lastRow)):
                if df_lastRow.loc[i, '名称'] in [0.0, '0.0', '0']:
                    targetRow = df[df['日期'] == df_lastRow.loc[i, '日期']]
                    if len(targetRow)>0:
                        sqlStr  = 'Update ' + subjectname + '\n'
                        sqlStr += 'SET '
                        for column in df_lastRow.columns:
                            if column == '名称':
                                sqlStr += column + '=\'' +\
                                    str(list(targetRow[column])[0]) +'\','
                            else:
                                sqlStr += column + '=' + str(list(targetRow[column])[0]) +','
                        sqlStr = sqlStr[:-1] + '\n'
                        sqlStr += 'WHERE 日期='+str(list(targetRow['日期'])[0])
                        # print(sqlStr)
                        cur.execute(sqlStr)
        self.writing.value == 0

    
    def getDF(self, subjectname):
        print('Enter getDF')
        conn = self.create_connection_for_read()
        df = pd.read_sql_query("SELECT * from " + subjectname, conn)
        # df = df.iloc[::-1].reset_index(drop=True)
        conn.close()
        print('Exit getDF')
        return df
    
    def getNumRows(self, subjectname):
        arg = 'Select COUNT(*) From ' + subjectname
        numRow = self.execute(arg)
        return list(numRow)[0][0]
    
    def getLastRowsFromConn(self, subjectname, conn, numRow=1):
        if subjectname in self.subjectnames:
            df = pd.read_sql_query("SELECT * from " + subjectname +\
                                   " ORDER BY rowid DESC LIMIT " + str(numRow), conn)
            df = df.iloc[::-1].reset_index(drop=True)
        else:
            df = []
            print("{} is not in the database".format(subjectname))
        return df
    
    def getLastRows(self, subjectname, numRow=1):
        # print('Enter getLastRows')
        conn = self.create_connection_for_read()
        df = self.getLastRowsFromConn(subjectname, conn, numRow=numRow)
        conn.close()
        # print('Exit getLastRows')
        return df
    
    def resetSubject(self, subjectname, df):
        conn = self.create_connection_for_write()
        self.writing.value == 1
        df.to_sql(subjectname, con=conn, if_exists='replace', index=False, method='multi')
        conn.close()
        self.writing.value == 0
        
    def updateSubjects(self, targetObjs=[]):
        if len(targetObjs) == 0:
            targetObjs = self.tradeObjs
        pool = mp.get_context("spawn").Pool(8)
        # pool = mp.dummy.Pool(8)
        # targetObjs_temp = []
        # for subobj in targetObjs:
        #     print("Working on {}".format(subobj.subjectname))
        #     targetObjs_temp.append(Subject.updateSubject(subobj))
        print("Pool to updateSubjects")
        targetObjs = pool.map(Subject.updateSubject, targetObjs)
        print("Pool to updateSubjects done")
        # targetObjs = targetObjs_temp
        return targetObjs
    
    
    def setLastUpdatedDate(self, subobj, conn):
        lastRow = self.getLastRowsFromConn(subobj.subjectname, conn)
        lastDate = list(lastRow[subobj.DateStr])[0]
        subobj.lastUpdatedDate = dateStrToDateTime(numberToStr(lastDate))
    
    def writeSubjects(self, tradeObjs=[]):
        conn = self.create_connection_for_write()
        self.writing.value == 1
        if len(tradeObjs) == 0:
            tradeObjs = self.tradeObjs
        for subobj in tradeObjs:
            if subobj.hasNewContent:
                # print("Writing on {}".format(subobj.subjectname))
                self.writeSubjectFromConn(subobj.subjectname, subobj.newContents, conn)
                subobj.newContents = []
                subobj.hasNewContent = False
                self.setLastUpdatedDate(subobj, conn)
        self.writing.value == 0
    
    def writeSubject(self, subjectname, df):
        # print('Enter appendSubject')
        conn = self.create_connection_for_write()
        self.writing.value == 1
        self.writeSubjectFromConn(subjectname, df, conn)
        self.writing.value == 1
        # print('Exit appendSubject')
        
    def writeSubjectFromConn(self, subjectname, df, conn):
        DateStr = self.objMap[subjectname].DateStr
        df_lastRow = self.getLastRowsFromConn(subjectname, conn)
        date_lastRow = dateStrToDateTime(str(df_lastRow.loc[0, DateStr]))
        dates = df[DateStr].apply(str)
        dates = dates.apply(dateStrToDateTime)
        dates_compared = list(dates > date_lastRow)
        if any(dates_compared):
            index_lastRow = dates_compared.index(True)
            df_new = df[index_lastRow:]
            df_new.to_sql(subjectname, con=conn, if_exists='append', index=False)
        
    
    def insertSubject(self, subjectname):
        if subjectname in self.subjectnames:
            return
        if 'S' in subjectname:
            isStock = True
        else:
            isStock = False
        subobj = Subject(subjectname[1:], self, isStock=isStock)
        subobj.resetFlag=True
        # pool = mp.get_context("spawn").Pool(8)
        pool = mp.dummy.Pool(8)
        subobj = Subject.updateSubject(subobj, pool=pool)
        self.resetSubject(subjectname, subobj.newContents)
        subobj.setLastUpdatedDate(self)
        self.subjectnames.append(subjectname)
        self.objMap[subjectname] = subobj
    
    def monitorTradeSystem(self):
        while True:
            print(self.getLastRows('S000985', 5))
            sleep(10)
            

if __name__=='__main__':
    # xlsx_path = 'Resources.xlsx'
    db_path = 'Resources.db'
    sql = Database(db_path)
    # sql.createDB(xlsx_path, db_path)
    print(sql.getLastRows('S000985', 10))
    sleep(5)
    # sql.run()
    sleep(5)
    # from SelfTradingSystem.io.subject import Subject
    # from multiprocessing.dummy import Pool as ThreadPool
    print(sql.getLastRows('S000985', 10))
    # print('-'*20)
    # subjectname = 'S000688'
    # print("Try to insert {}".format(subjectname))
    # subobj = Subject(subjectname[1:], sql, isStock=True)
    # pool = ThreadPool(8)
    # subobj.resetFlag=True
    # pool = mp.dummy.Pool(8)
    # subobj = Subject.updateSubject(subobj, pool=pool)
    # sql.insertSubject(subjectname)
    