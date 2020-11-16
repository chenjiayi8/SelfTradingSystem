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

class Database(mp.Process):
    def __init__(self, db, pool):
        super(Database, self).__init__()
        self.db=db
        self.pool = pool
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
            print("Connection to SQLite DB for read successful")
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
        if  nowTimeTuple.tm_hour >= 10 and nowTimeTuple.tm_hour < self.tradingHour:
            print("Start updating loop")
            hasNewContent = False
            tradeObjs = []
            for subjectname in self.subjectnames:
                subobj = self.objMap[subjectname]
                if subobj.lastUpdatedDate < todaydate:
                    tradeObjs.append(subobj)
            # callBatchMethod(self, 'updateSubjects')
            self.updateSubjects(tradeObjs)
            print("updateSubjects done")
            for subjectname in self.subjectnames:
                subObj = self.objMap[subjectname]
                if not hasNewContent and subObj.hasNewContent:
                    hasNewContent = True
            if hasNewContent:
                print("Has new content")
                self.writeSubjects()
                print("writeSubjects done")
        else:
            print("Not updating during target hours")
        # if nowTimeTuple.tm_wday+1 in [6, 7]:
        # callBatchMethod(self, 'validateZZQZ')
        self.validateZZQZ()
        sleep(60)
        print('Exit keepUpdating')
    
    def validateZZQZ(self):
        subobj = self.objMap['S000985']
        if subobj.validatedDate < subobj.lastUpdatedDate:
            df = subobj.getValidatedZZQZ()
            subjectname = 'S000985'
            df_lastRow = self.getLastRows(subjectname, 20)
            conn = self.create_connection_for_write()
            self.writing.value == 1
            cur = conn.cursor()
            for i in range(len(df_lastRow)):
                if str.isnumeric(df_lastRow.loc[i, '名称']):
                    targetRow = df[df['日期'] == df_lastRow.loc[i, '日期']]
                    if len(targetRow)>0:
                        sql  = 'Update ' + subjectname + '\n'
                        sql += 'SET '
                        for column in df_lastRow.columns:
                            if column == '名称':
                                sql += column + '=\'' +\
                                    str(list(targetRow[column])[0]) +'\','
                            else:
                                sql += column + '=' + str(list(targetRow[column])[0]) +','
                        sql = sql[:-1] + '\n'
                        sql += 'WHERE 日期='+str(list(targetRow['日期'])[0])
                        # print(sql)
                        cur.execute(sql)
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
        df = pd.read_sql_query("SELECT * from " + subjectname +\
                               " ORDER BY rowid DESC LIMIT " + str(numRow), conn)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    
    def getLastRows(self, subjectname, numRow=1):
        print('Enter getLastRows')
        conn = self.create_connection_for_read()
        df = self.getLastRowsFromConn(subjectname, conn, numRow=numRow)
        conn.close()
        print('Exit getLastRows')
        return df
    
    def resetSubject(self, subjectname, df):
        conn = self.create_connection_for_write()
        self.writing.value == 1
        df.to_sql(subjectname, con=conn, if_exists='replace', index=False, method='multi')
        conn.close()
        self.writing.value == 0
        
    def updateSubjects(self, tradeObjs=[]):
        if len(tradeObjs) == 0:
            tradeObjs = self.tradeObjs
        for subobj in tradeObjs:
            print("Working on {}".format(subobj.subjectname))
            Subject.updateSubject(subobj)
        # print("Pool to updateSubjects")
        # self.pool.map(Subject.updateSubject, tradeObjs)
    
    
    def setLastUpdatedDate(self, subobj, conn):
        lastRow = self.getLastRowsFromConn(subobj.subjectname, conn)
        lastDate = list(lastRow[subobj.DateStr])[0]
        subobj.lastUpdatedDate = dateStrToDateTime(numberToStr(lastDate))
    
    def writeSubjects(self):
        conn = self.create_connection_for_write()
        self.writing.value == 1
        for subobj in self.tradeObjs:
            if subobj.hasNewContent:
                self.writeSubjectFromConn(subobj.subjectname, subobj.newContents, conn)
                self.newContents = []
                self.hasNewContent = False
                self.setLastUpdatedDate(subobj, conn)
        self.writing.value == 0
    
    def writeSubject(self, subjectname, df):
        print('Enter appendSubject')
        conn = self.create_connection_for_write()
        self.writing.value == 1
        self.writeSubjectFromConn(subjectname, df, conn)
        self.writing.value == 1
        print('Exit appendSubject')
        
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
        
    
    def monitorTradeSystem(self):
        while True:
            print(self.getLastRows('S000985', 5))
            sleep(10)
            

if __name__=='__main__':
    xlsx_path = 'Resources.xlsx'
    db_path = 'Resources.db'
    pool = mp.get_context("spawn").Pool()
    sql =Database(db_path, pool)
    sql.createDB(xlsx_path, db_path)
    print(sql.getLastRows('S000985', 10))

    