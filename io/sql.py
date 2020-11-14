# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 11:06:07 2020

@author: jiayichen
"""


import sqlite3
import pandas as pd
from sqlite3 import Error
from TestSQLMultithreading import MultiThreadOK
from datetime import datetime
import multiprocessing as mp
import time
import random


lock = mp.Value('i', 0)

def sleep(seconds): #for KeyboardInterrupt 
    for i in range(seconds):
        time.sleep(1)
        
def dateStrToDateTime(dateStr):
    return datetime.strptime(dateStr, "%Y%m%d")

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


def selectLastNRow(conn, sheetname, numRow=1):
    if lock.value != 0:
        sleep(10)
        return selectLastNRow(conn, sheetname, numRow)
    print('Enter selectLastNRow')
    lock.value = 1
    df = pd.read_sql_query("SELECT * from " + sheetname +\
                           " ORDER BY rowid DESC LIMIT " + str(numRow), conn)
    df = df.iloc[::-1].reset_index(drop=True)
    lock.value = 0
    print('Exit selectLastNRow')
    return df


def appendSQL(conn, sheetname, df):
    if lock.value != 0:
        return
    df_lastRow = selectLastNRow(conn, sheetname)
    print('Enter appendSQL')
    lock.value = 1
    date_lastRow = dateStrToDateTime(str(df_lastRow.loc[0, '日期']))
    dates = df['日期'].apply(str)
    dates = dates.apply(dateStrToDateTime)
    dates_compared = list(dates > date_lastRow)
    if any(dates_compared):
        index_lastRow = dates_compared.index(True)
        df_new = df[index_lastRow:]
        df_new.to_sql(sheetname, con=conn, if_exists='append', index=False)
    lock.value = 0
    print('Exit appendSQL')


def updateSQL(conn, sheetname, df):
    if lock.value != 0:
        return
    df_lastRow = selectLastNRow(conn, sheetname, 30)
    print("Enter updateSQL")
    lock.value = 1
    cur = conn.cursor()
    for i in range(len(df_lastRow)):
        if str.isnumeric(df_lastRow.loc[i, '名称']):
            targetRow = df[df['日期'] == df_lastRow.loc[i, '日期']]
            if len(targetRow)>0:
                sql  = 'Update ' + sheetname + '\n'
                sql += 'SET '
                for column in df_lastRow.columns:
                    if column == '名称':
                        sql += column + '=\'' +\
                            str(list(targetRow[column])[0]) +'\','
                    else:
                        sql += column + '=' + str(list(targetRow[column])[0]) +','
                sql = sql[:-1] + '\n'
                sql += 'WHERE 日期 = '+str(list(targetRow['日期'])[0])
                # print(sql)
                cur.execute(sql)
    lock.value = 0
    print("Exit updateSQL")
#    date_lastRow = dateStrToDateTime(str(df_lastRow.loc[0, '日期']))


def keepIndexUpdated():
    global conn
    while True:
        df_raw = pd.read_excel('指数和基金.xlsx', sheet_name='S000985')
        appendSQL(conn, 'S000985', df_raw)
        updateSQL(conn, 'S000985', df_raw)
        sleep(random.randint(5, 10))

def monitorTradeSystem():
    global conn
    while True:
        print(selectLastNRow(conn, 'S000985', 5))
        sleep(10)

if __name__ == '__main__':
    db_path = '本金账本.db'
    xlsx_path = '本金账本1.xlsx'
    conn = create_connection(db_path)
    dfs = pd.read_excel(xlsx_path, sheet_name=None)

    starttime = datetime.now()
    for table, df in dfs.items():
        df.to_sql(table, con=conn, if_exists='replace', index=False, method='multi')
    
    print((datetime.now() - starttime).seconds)
    # print(selectLastNRow(conn, 'S000985', 30))
    # df_raw = pd.read_excel('指数和基金.xlsx', sheet_name='S000985')
    # appendSQL(conn, 'S000985', df_raw)
    # print(selectLastNRow(conn, 'S000985', 30))
    # updateSQL(conn, 'S000985', df_raw)
    # print(selectLastNRow(conn, 'S000985', 30))
    
    # p1 = mp.Process(target=keepIndexUpdated)
    # p1.start()
    # p2 = mp.Process(target=monitorTradeSystem)
    # p2.start()
    
    conn.close()

