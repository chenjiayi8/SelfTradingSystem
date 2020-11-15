#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 00:54:26 2020

@author: frank
"""
import tabulate as tb
from SelfTradingSystem.io.database import Database

def printTable(table):
    msg = tb.tabulate(table.values, table.columns, tablefmt="pipe")
    print(msg)
    return msg


xlsx = 'Resources.xlsx'
db = 'Resources.db'
sql =Database(db)
printTable(sql.getLastRows('S000985', 10))
sql.close()

