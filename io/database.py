# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 11:26:23 2020

@author: jiayichen
"""

from SelfTradingSystem.util.others import sleep
import sqlite3
from sqlite3 import Error
import pandas as pd
import multiprocessing as mp


class Database(mp.Process):
    def __init__(self, db):
        super(Database, self).__init__()
        self.db=db
        self.running = mp.Value('i', 0)
        self.start()
    def run(self):
        conn = self.create_connection() 
        self.running.value = 1
        while self.running.value == 1:
            self.keepUpdating(conn)
        conn.close()
        
    def close(self):
        self.running.value = 0
        
    def create_connection(self):
        connection = None
        try:
            connection = sqlite3.connect(self.db)
            print("Connection to SQLite DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        return connection
    
    def execute(self, arg):
        conn = self.create_connection()
        cursor = conn.cursor()
        cursor.execute(arg)
        results = cursor.fetchall()
        conn.close()
        return results
    
    def createDB(self, xlsx_path, db_path):
        self.db = db_path
        conn = self.create_connection()
        dfs = pd.read_excel(xlsx_path, sheet_name=None)
        for table, df in dfs.items():
            df.to_sql(table, con=conn, if_exists='replace', index=False, method='multi')
        conn.close()
        
    def keepUpdating(self, conn):
        print('Enter keepUpdating')
        sleep(10)
        print('Exit keepUpdating')
        pass
    
    def getDF(self, sheetname):
        print('Enter getDF')
        conn = self.create_connection()
        df = pd.read_sql_query("SELECT * from " + sheetname, conn)
        # df = df.iloc[::-1].reset_index(drop=True)
        conn.close()
        print('Exit getDF')
        return df
    
    def getNumRows(self, sheetname):
        arg = 'Select COUNT(*) From ' + sheetname
        numRow = sql.execute(arg)
        return list(numRow)[0][0]
    
    def getLastRows(self, sheetname, numRow=1):
        print('Enter getLastRows')
        conn = self.create_connection()
        df = pd.read_sql_query("SELECT * from " + sheetname +\
                               " ORDER BY rowid DESC LIMIT " + str(numRow), conn)
        df = df.iloc[::-1].reset_index(drop=True)
        conn.close()
        print('Exit getLastRows')
        return df
    
    def monitorTradeSystem(self):
        while True:
            print(self.getLastRows('S000985', 5))
            sleep(10)
            
    def updateSubjects(self):
        pass

if __name__=='__main__':
    xlsx = 'Resources.xlsx'
    db = 'Resources.db'
    sql =Database(db)
    # arg = 'Select ROW_NUMBER() From S000985 OVER (ORDER BY rowid DESC LIMIT 1) '
    # arg = 'SELECT ROW_NUMBER() OVER (Select * from S000985 ORDER BY rowid DESC LIMIT 1)'
    arg = 'Select COUNT(*) From S000985'
    numRow = sql.execute(arg)
    print(sql.getNumRows('S000985'))
# ‘Select ROW_NUMBER() OVER (PARTITION BY rowid ORDER BY rowid DESC LIMIT 1),