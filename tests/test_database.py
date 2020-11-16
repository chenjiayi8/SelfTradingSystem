#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 00:54:26 2020

@author: frank
"""
import tabulate as tb
import multiprocessing as mp
from SelfTradingSystem.io.database import Database

def printTable(table):
    msg = tb.tabulate(table.values, table.columns, tablefmt="pipe")
    print(msg)
    return msg


if __name__=='__main__':
    xlsx_path = 'Resources.xlsx'
    db_path = 'Resources.db'
    pool = mp.get_context("spawn").Pool()
    sql =Database(db_path, pool)
    sql.createDB(xlsx_path, db_path)
    print(sql.getLastRows('S000985', 10))

    