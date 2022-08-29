# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 11:26:23 2020

@author: jiayichen
"""

from SelfTradingSystem.util.others import sleep, xirr
from SelfTradingSystem.util.convert import (
    dateStrToDateTime, numberToStr, getTodayDate, dateTimeToDateStr,
    dateStrToDate
    )
from SelfTradingSystem.io.subject import Subject, updateRelativeMomentumWrapper
# from SelfTradingSystem.io.excel import excelToDFs
from SelfTradingSystem.util.stock import getStockName
import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np
from  statistics import median, mean
import multiprocessing as mp
from collections import OrderedDict
# try:
#     mp.set_start_method('spawn')
# except:
#     pass
import atexit
# import datetime
import sys
import traceback
# import os
from pandas.tseries.offsets import BDay
from datetime import datetime
# from datetime import timedelta

Tplus2DayList = ['F513030',
            'F513500',
            'F513100',
            'F513050',
            'F162411',
            'F001063',
            ]

def needToUpdate(subobj, todaydate):
    if subobj.subjectname in Tplus2DayList:
        return subobj.lastUpdatedDate <  (todaydate - BDay(1))
    else:
        return subobj.lastUpdatedDate < todaydate

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
    def __init__(self, db, updatingHour=15, tradingHour=20):
        self.db=db
        # self.pool = mp.get_context("spawn").Pool(8)
        self.pool = mp.dummy.Pool(8)
        self.updatingHour = updatingHour
        self.tradingHour = tradingHour
        self.running = mp.Value('i', 0)
        self.writing = mp.Value('i', 0)
        self.batchMethods = {}
        self.batchMethods['updateSubjects'] = self.updateSubjects
        # self.batchMethods['validateZZQZ'] = self.validateZZQZ
        self.setSubjectNames()
        self.initialSubjects()
        self.setMenuSheet()

        
    def setSubjectNames(self):
        arg = ('SELECT name from sqlite_master where type= "table"')
        names = list(self.execute(arg))
        names = [name[0] for name in names]
        if 'Menu' in names:
            names.remove('Menu')
        self.subjectnames = names
        df_menu = self.getDF('Menu')
        self.chineseNameDict = OrderedDict()
        chineseNames = list(df_menu['中文名'])
        subjectNames = list(df_menu['代码'])
        for i in range(len(chineseNames)): self.chineseNameDict[subjectNames[i]] = chineseNames[i]
    
    def initialSubjects(self):
        conn = self.create_connection_for_read()
        self.tradeObjs = []
        for name in self.subjectnames:
            if 'S' in name:
                isStock = True
            else:
                isStock = False
            subobj = Subject(name[1:], self, isStock=isStock)
            self.setLastUpdatedDate(subobj, conn)
            self.tradeObjs.append(subobj)
        self.objMap = dict(zip(self.subjectnames, self.tradeObjs))
        conn.close()
    
    def setMenuSheet(self):
        df = self.getDF('Menu')
        df_origin = df.copy()
        sheetnames_menu = list(df['代码'])
        for sheetname in self.subjectnames:
            if sheetname not in sheetnames_menu:
                data = {'代码': sheetname,
                        '中文名': getStockName(sheetname),
                        '更新日期': dateTimeToDateStr(self.objMap[sheetname].lastUpdatedDate)
                        }
                df = df.append(data, ignore_index=True)
            else:
                df.loc[df['代码'] == sheetname, '更新日期'] = dateTimeToDateStr(self.objMap[sheetname].lastUpdatedDate)
        if not df.equals(df_origin):#only save when change is made
            self.resetSubject('Menu', df)
    
    
    def run(self):
        self.running.value = 1
        while self.running.value == 1:
            self.keepUpdating()
        
    def close(self):
        self.running.value = 0
        
    def create_connection_for_read(self):
        connection = None
        try:
            connection = sqlite3.connect(self.db, isolation_level=None,\
                                     timeout=10, check_same_thread=False)
            atexit.register(connection.close)
            # print("Connection to SQLite DB for read successful")
        except Error as e:
            print("The error '{e}' occurred".format(e=e))
        return connection
    
    
    def create_connection_for_write(self):
        connection = None
        while self.writing.value == 1:
            print("Waiting for writing lock")
            sleep(10)
        try:
            connection = sqlite3.connect(self.db, isolation_level=None,\
                                         timeout=10, check_same_thread=False)
            connection.execute('pragma journal_model=wal')
            atexit.register(connection.close)
            print("Connection to SQLite DB for write successful")
        except Error as e:
            print("The error '{e}' occurred".format(e=e))
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
        

    def forceUpdating(self):
        todaydate =  getTodayDate()
        targetObjs = []
        for subjectname in self.subjectnames:
            subobj = self.objMap[subjectname]
            if needToUpdate(subobj, todaydate):
                targetObjs.append(subobj)
        if len(targetObjs) > 0:
             print("Before updateSubjects")
             targetObjs = self.updateSubjects(targetObjs)
             print("updateSubjects done")
             tradedObjs = [subobj for subobj in targetObjs if subobj.hasNewContent]
             if len(tradedObjs) > 0:
                 print("Has new content")
                 self.writeSubjects(tradedObjs)
                 print("writeSubjects done")
                 self.setMenuSheet()
        else:
             print("No need to update anymore")   
        # callBatchMethod(self, 'validateZZQZ')
                   
    def forceUpdatingTarget(self):
        targets = ['S399995', 'S000989']
        todaydate =  getTodayDate()
        targetObjs = []
        for subjectname in targets:
            subobj = self.objMap[subjectname]
            if needToUpdate(subobj, todaydate):
                targetObjs.append(subobj)
        if len(targetObjs) > 0:
            print("Enter forceUpdatingTarget")
            print("Before updateSubjects")
            targetObjs = self.updateSubjects(targetObjs)
            print("updateSubjects done")
            tradedObjs = [subobj for subobj in targetObjs if subobj.hasNewContent]
            if len(tradedObjs) > 0:
                print("Has new content")
                self.writeSubjects(tradedObjs)
                print("writeSubjects done")
                self.setMenuSheet()
            print("Exit forceUpdatingTarget")
    
    
    def keepUpdating(self):
        print('Enter keepUpdating')
        nowTime = datetime.now()
        nowTimeTuple = nowTime.timetuple()
        if nowTimeTuple.tm_wday < 6 and nowTimeTuple.tm_hour >= self.updatingHour\
            and nowTimeTuple.tm_hour < self.tradingHour:
            print("Start updating loop")
            # self.forceUpdatingTarget()
            self.forceUpdating()
        else:
            print("Not updating before {} and after {} at workdays".format(self.updatingHour, self.tradingHour))

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
            conn.close()
            self.writing.value == 0
            subobj.setValidatedDate(self)

    
    def getDF(self, subjectname):
        print('Enter getDF '+ subjectname)
        conn = self.create_connection_for_read()
        df = pd.read_sql_query("SELECT * from " + subjectname, conn)
        # df = df.iloc[::-1].reset_index(drop=True)
        conn.close()
        print('Exit getDF '+ subjectname)
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
    
    def resetSubjectWithoutDF(self, subjectname):
        subobj = self.objMap[subjectname]
        subobj.resetFlag=True
        subobj = Subject.updateSubject(subobj, pool=self.pool)
        df = subobj.newContents
        self.resetSubject(subjectname, df)
    
    def resetSubject(self, subjectname, df):
        conn = self.create_connection_for_write()
        self.writing.value == 1
        df.to_sql(subjectname, con=conn, if_exists='replace', index=False)#, method='multi')
        conn.commit()
        conn.close()
        self.writing.value == 0
        
    def updateSubjects(self, targetObjs=[]):
        if len(targetObjs) == 0:
            targetObjs = self.tradeObjs
        # pool = mp.get_context("spawn").Pool(8)
        # pool = mp.dummy.Pool(8)
        # targetObjs_temp = []
        # for subobj in targetObjs:
        #     print("Working on {}".format(subobj.subjectname))
        #     targetObjs_temp.append(Subject.updateSubject(subobj))
        print("Pool to updateSubjects")
        targetObjs = self.pool.map(Subject.updateSubject, targetObjs)
        print("Pool to updateSubjects done")
        # targetObjs = targetObjs_temp
        return targetObjs
    
    def modifySubject(self, subjectname, modifications):
        conn = self.create_connection_for_write()
        self.writing.value == 1
        cur = conn.cursor()
        for m in modifications:
            col, rowId, content = m
            selfStr  = 'Update ' + subjectname + '\n'
            selfStr += 'SET "{}"="{}"\n'.format(col,content)
            selfStr += 'WHERE rowid={}'.format(rowId)
            print(selfStr)
            cur.execute(selfStr)
        conn.close()
        self.writing.value == 0
        
    def modifySubjects(self, modifications):
        conn = self.create_connection_for_write()
        self.writing.value == 1
        cur = conn.cursor()
        for m in modifications:
            subjectname, col, rowId, content = m
            selfStr  = 'Update ' + subjectname + '\n'
            selfStr += 'SET "{}"="{}"\n'.format(col,content)
            selfStr += 'WHERE rowid={}'.format(rowId)
            print(selfStr)
            cur.execute(selfStr)
        conn.close()
        self.writing.value == 0

    
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
                if subobj.subjectname in ['S000985', 'S399995', 'S000989']:
                    print("Will Write {} from {}".format(subobj.subjectname,
                                              dateTimeToDateStr(subobj.lastUpdatedDate)))
                self.writeSubjectFromConn(subobj.subjectname, subobj.newContents, conn)
                subobj.newContents = []
                subobj.hasNewContent = False
                self.setLastUpdatedDate(subobj, conn)
                if subobj.subjectname in ['S000985', 'S399995', 'S000989']:
                    print("Writing {} done from {}".format(subobj.subjectname,
                                              dateTimeToDateStr(subobj.lastUpdatedDate)))
        conn.close()
        self.writing.value == 0
    
    def writeSubject(self, subjectname, df):
        # print('Enter appendSubject')
        conn = self.create_connection_for_write()
        self.writing.value == 1
        self.writeSubjectFromConn(subjectname, df, conn)
        conn.close()
        self.writing.value == 0
        # print('Exit appendSubject')
        
    def writeSubjectFromConn(self, subjectname, df, conn):
        DateStr = self.objMap[subjectname].DateStr
        df_lastRow = self.getLastRowsFromConn(subjectname, conn)
        try:
            df = df[df_lastRow.columns].copy()
        except:
            print(subjectname+':')
            print("SQL", df_lastRow.columns)
            print("New", df.columns)
            raise
        assert all(df_lastRow.columns == df.columns),\
            "Unmatched columns for "+subjectname
            
        date_lastRow = dateStrToDateTime(str(df_lastRow.loc[0, DateStr]))
        dates = df[DateStr].apply(str)
        dates = dates.apply(dateStrToDateTime)
        dates_compared = list(dates > date_lastRow)
        if any(dates_compared):
            index_lastRow = dates_compared.index(True)
            df_new = df[index_lastRow:]
            df_new.to_sql(subjectname, con=conn, if_exists='append', index=False)
        
    def deleteSubject(self, subjectname):
        df = self.getDF('Menu')
        conn = self.create_connection_for_write()
        # Get a cursor object
        cursor = conn.cursor()
        # Execute the DROP Table SQL statement
        dropTableStatement = "DROP TABLE " + subjectname
        cursor.execute(dropTableStatement)
        conn.close()
        df.drop(df[df['代码']==subjectname].index, inplace=True)
        self.resetSubject('Menu', df)
    
    
    def insertSubject(self, subjectname):
        if subjectname in self.subjectnames:
            self.deleteSubject(subjectname)
        if 'S' in subjectname:
            isStock = True
        else:
            isStock = False
        subobj = Subject(subjectname[1:], self, isStock=isStock)
        subobj.resetFlag=True
        # pool = mp.get_context("spawn").Pool(8)
        # pool = mp.dummy.Pool(8)
        subobj = Subject.updateSubject(subobj, pool=self.pool)
        if len(subobj.newContents) > 0:
            self.resetSubject(subjectname, subobj.newContents)
            self.subjectnames.append(subjectname)
            subobj.setLastUpdatedDate(self)
            self.objMap[subjectname] = subobj
            self.setMenuSheet()
        else:
            print("Cannot get the contents for {}".format(subjectname))
    
    
    def checkInvalidFundP(self):
        with self.create_connection_for_read() as conn_read:
            results = []
            for table in self.subjectnames:
                if 'F' in table:
                    arg = 'SELECT * from {} where 日增长率 = ""'.format(table)
                    df = pd.read_sql_query(arg, con=conn_read)
                    if len(df) > 0:
                        df['Table'] = table
                        results.append(df)       
        if len(results) > 0:    
            df_final = pd.concat(results, ignore_index=True)
            df_final['净值日期'] = df_final['净值日期'].apply(str)
            columns = list(df_final.columns)
            columns.remove('Table')
            sqlTasks = []
            with self.create_connection_for_read() as conn_read:
                cur = conn_read.cursor()
                for result in results:
                    table = list(set(result['Table']))[0]
                    for i in range(len(result)):
                        dateStr = str(result.loc[i, '净值日期'])
                        date    = dateStrToDateTime(dateStr)
                        datePre = date - BDay(5)
                        datePreStr = dateTimeToDateStr(datePre)
                        arg = 'SELECT * from {} where 净值日期  > {} and 净值日期  <= {}'.format(table, datePreStr, dateStr)
                        cur.execute(arg)
                        df =  pd.DataFrame(data=cur.fetchall(), columns=columns)
                        TCloseLast   = float(df['累计净值'].iloc[-2])
                        TClose   =  float(df['累计净值'].iloc[-1])
                        TCloseChange = round(TClose/TCloseLast-1, 4)
                        # TClose    = round(TCloseLast*(1+TCloseChange), 4)
                        sqlStr  = 'Update ' + table + '\n'
                        sqlStr += 'SET 日增长率={}\n'.format(TCloseChange)
                        sqlStr += 'WHERE 净值日期='+dateStr
                        sqlTasks.append(sqlStr)
                        df_final.loc[(df_final['Table']==table) & (df_final['净值日期'] == dateStr), '日增长率'] = TCloseChange
            print("These 日增长率 are corrected:")
            print(df_final)
            with self.create_connection_for_write() as conn_write:
                cur_write = conn_write.cursor()
                for sqlStr in sqlTasks: cur_write.execute(sqlStr)
    
    def checkInvalidFundValues(self):
        results = []
        with self.create_connection_for_read() as conn_read:
            for table in self.subjectnames:
                    if 'F' in table:
                        arg = 'SELECT * from {} where 累计净值 = ""'.format(table)
                        df = pd.read_sql_query(arg, con=conn_read)
                        if len(df) > 0:
                            df['Table'] = table
                            results.append(df)       
        if len(results) > 0:    
            df_final = pd.concat(results, ignore_index=True)
            df_final['净值日期'] = df_final['净值日期'].apply(str)
            # columns = list(df_final.columns)
            # columns.remove('Table')
            selfTasks = []
            for result in results:
                table = list(set(result['Table']))[0]
                for i in range(len(result)):
                    dateStr = str(result.loc[i, '净值日期'])
                    date    = dateStrToDateTime(dateStr)
                    datePre = date - BDay(10)
                    # datePreStr = dateTimeToDateStr(datePre)
                    # arg = 'SELECT * from {} where 净值日期  > {} and 净值日期  <= {}'.format(table, datePreStr, dateStr)
                    # cur.execute(arg)
                    df = self.getDF(table)
                    df['净值日期'] = df['净值日期'].apply(dateStrToDateTime)
                    df = df[(df['净值日期'] > datePre.to_pydatetime()) & (df['净值日期'] < date)]
                    df = df.copy().reset_index(drop=True)
                    # df = df.res
                    # df =  pd.DataFrame(data=cur.fetchall())
                    TCloseLast   = float(df['累计净值'].iloc[-2])                            
                    TCloseChange = float(df['日增长率'].iloc[-1])
                    TClose    = round(TCloseLast*(1+TCloseChange), 4)
                    selfStr  = 'Update ' + table + '\n'
                    selfStr += 'SET 累计净值={}\n'.format(TClose)
                    selfStr += 'WHERE 净值日期='+dateStr
                    # print(selfStr)
                    selfTasks.append(selfStr)
                    df_final.loc[(df_final['Table']==table) & (df_final['净值日期'] == dateStr), '累计净值'] = TClose
            print("These 累计净值 are corrected:")
            print(df_final)
            with self.create_connection_for_write() as conn_write:
                cur_write = conn_write.cursor()
                for selfStr in selfTasks: cur_write.execute(selfStr)  
    
    def checkInvalidFund(self):
        self.checkInvalidFundValues()
        self.checkInvalidFundP()
    
    def updateMomentums(self, tradeObjs=[]):
        if len(tradeObjs) == 0:
            tradeObjs = self.tradeObjs
        for obj in tradeObjs:
            obj.preCondition(self)
        baseObj = self.objMap['S000985']
        inputs = [[subObj, baseObj] for subObj in tradeObjs]
        # print("Finishing precondition")
        momentum_results = self.pool.map(Subject.updateMomentum, tradeObjs)
        relativeMomentum_results = self.pool.map(updateRelativeMomentumWrapper, inputs)
        columns = ['N天涨幅','T','T-1','T-2','T-3','T-4','T-5','T-6','T-7']
        df_menu = self.getDF('Menu')
        df_menu.index = df_menu['代码']
        df_origin = df_menu.copy()
        df_menu['上周位置'] = df_menu['52周位置']
        for i in range(len(tradeObjs)):
            momentum_result = momentum_results[i]
            subjectname = tradeObjs[i].subjectname
            if len(momentum_result) > 0:
                df_menu.loc[subjectname, '趋势'] = momentum_result[0]
                df_menu.loc[subjectname, '52周位置'] = momentum_result[1]
            df_menu.loc[subjectname, columns] = relativeMomentum_results[i]
            
        if not df_menu.equals(df_origin):#only save when change is made
            self.resetSubject('Menu', df_menu)
            
    def updateQuarterPerformance(self):
        df_menu = sql.getDF('Menu')
        df_menu.index = df_menu['代码']
        df_origin = df_menu.copy()
        df_menu['季度中位涨幅'] = 0
        df_menu['季度平均涨幅'] = 0
        df_menu['年华收益率'] = 0
        for subjectname in df_menu.index:
            # df = sql.getDF(subjectname)
            subobj = sql.objMap[subjectname]
            subobj.preCondition(sql, 10000)
            df_month = Subject.getMonthDF(subobj.preConditionedDF)
            df_month['QuarterID'] = df_month[['Year', 'Month']].\
            apply(lambda x: str(x['Year'])+'_'+str((x['Month']-1)//3+1), axis=1)
            df_quarter = df_month.drop_duplicates('QuarterID', keep="last").copy()
            df_quarter.drop(df_quarter.tail(1).index,inplace=True)
            TClose_quarter = np.array(df_quarter[subobj.TCloseStr])
            TClose_quarterP = TClose_quarter[1:]/TClose_quarter[0:-1]-1
            df_menu.loc[subjectname, '季度中位涨幅'] = '{}%'.\
            format(round(median(TClose_quarterP)*100, 2))
            df_menu.loc[subjectname, '季度平均涨幅'] = '{}%'.\
            format(round(mean(TClose_quarterP)*100, 2))
            start_date = dateStrToDate(str(subobj.preConditionedDF[subobj.DateStr].iloc[0]))
            start_value = subobj.preConditionedDF[subobj.TCloseStr].iloc[0]
            end_date = dateStrToDate(str(subobj.preConditionedDF[subobj.DateStr].iloc[-1]))
            end_value = subobj.preConditionedDF[subobj.TCloseStr].iloc[-1]
            df_menu.loc[subjectname, '年华收益率'] = \
                '{}%'.format(round(xirr([(start_date,-1*start_value),
                      (end_date, end_value)])*100, 2))
        if not df_menu.equals(df_origin):#only save when change is made
            self.resetSubject('Menu', df_menu)
            

if __name__=='__main__':
    # xlsx_path = 'Resources.xlsx'
    db_path = 'Resources.db'
    sql = Database(db_path)
    # subobj = sql.objMap['S399995']
    
    # from SelfTradingSystem.util.others import  readBug
    # filename = 'S000985_cfd3cf.pickle'
    # [subobj, startDate, sht_new_df, sht_appended] = readBug(filename)
    
    # sql.forceUpdating()
    # sheetnamee = 'S399995'
    # sql.resetSubjectWithoutDF('S399995')
    # newsubjects = ['S000989', 'S399995']
    # for subjectname in newsubjects:
        # sql.insertSubject(subjectname)
    # df_menu = sql.getDF('Menu')
    # targets = ['S399995', 'S000989']
    # tradeObjs = [sql.objMap[t] for t in targets]
    
    # tradedObjs = sql.updateSubjects(tradeObjs)
    # tradeObjs = sql.tradeObjs[:3]
    # sql.updateMomentums()
    # sql.checkInvalidFund()
    # sql.createDB(xlsx_path, db_path)
    # print(sql.getLastRows('S000985', 10))
    # df = sql.getDF('Menu')
    # df.loc[:, '更新日期'] = df.loc[:, '更新日期'].apply(dateStrToDateTime)
    # df_QDII = df[df['更新日期'] < datetime.datetime(2020, 12, 28)]
    # todaydate = getTodayDate()
    # for  _, subobj in sql.objMap.items():
    #     print("{} {} needs to update ? {}".format(subobj.subjectname, subobj.lastUpdatedDate, needToUpdate(subobj, todaydate)))
    # # sleep(5)
    # sql.run()
    # sleep(5)
    # from SelfTradingSystem.io.subject import Subject
    # from multiprocessing.dummy import Pool as ThreadPool
    # print(sql.getLastRows('S000985', 10))
    # print('-'*20)
    # subjectname = 'S399995'
    # print("Try to insert {}".format(subjectname))
    # subobj = Subject(subjectname[1:], sql, isStock=True)
    # from SelfTradingSystem.util.stock import getStockHistory
    # df = getStockHistory(subobj.name)
    # sql.resetSubject(subjectname, df)
    # pool = ThreadPool(8)
    # subobj.resetFlag=True
    
    # subobj.setLastUpdatedDate(sql)
    # subobj2 = Subject.updateSubject(subobj)
    # tradedObjs = [subobj2]
    # sql.writeSubjects(tradedObjs)
    # sql.setMenuSheet()
   
    # sql.insertSubject(subjectname)
    
