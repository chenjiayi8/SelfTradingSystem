#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:35:58 2020

@author: frank
"""

from datetime import datetime, timedelta, date
import time
import math
import lxml
import re
import os

def getYearFromDate(dateStr):
    return dateStrToDateStruc(str(dateStr)).tm_year

def getMonthFromDate(dateStr):
    return dateStrToDateStruc(str(dateStr)).tm_mon

def getWeekNumFromDate(dateStr):
    return dateStrToDateTime(str(dateStr)).strftime("%W")

def dateStrToDateTime(dateStr):
    return datetime.strptime(str(dateStr), "%Y%m%d")

def dateStrToDate(dateStr):
    temp = dateStrToDateTime(dateStr)
    return date(year=temp.year, month=temp.month, day=temp.day)

def dateStrToDateStruc(dateStr):
    return dateStrToDateTime(dateStr).timetuple()

def dateTimeToDateStr(date_time):
    return datetime.strftime(date_time, "%Y%m%d")

def dateTimeToEuroDateStr(date_time):
    return datetime.strftime(date_time, "%d/%m/%Y")


def datetimeToTimeStr(date_time):
    return datetime.strftime(date_time, "%Y%m%d_%H%M%S")

def timeStrToDateTime(timeStr):
    return datetime.strptime(timeStr, "%Y%m%d_%H%M%S")

def timeStrToDateStr(timeStr):
    return dateTimeToDateStr(timeStrToDateTime(timeStr))

def cellDateToEuroDateStr(cell_date):
    dateStr=dateTimeToDateStr(cell_date)
    return dateTimeToEuroDateStr(dateStrToDateTime(dateStr))

def getTodayDate():
    localTime =time.localtime()
    todayDateStr = str(localTime.tm_year) + str(localTime.tm_mon).zfill(2) + str(localTime.tm_mday).zfill(2)
    return dateStrToDateTime(todayDateStr)

def getTodayDateStr():
    return dateTimeToDateStr(getTodayDate())

def getTomorrowDateStr():
    return dateTimeToDateStr(getTomorrowDate())

def getTomorrowDate():
    todayDate = getTodayDate()
    return todayDate+timedelta(days=1)

def getDeltaDateStr(ndays):
    todayDate = getTodayDate()
    return dateTimeToDateStr(todayDate+timedelta(days=ndays))

def getNowTimeStr():
    return datetimeToTimeStr(datetime.now())
    
def getDaysBetweenDateStrs(dateStr1, dateStr2):
    date1 = dateStrToDateTime(dateStr1)
    date2 = dateStrToDateTime(dateStr2)
    return (date1-date2).days

def getStockNumberStr(stockStr):
    return stockStr[2:]


def numberToStr(inputNumber):
    type_input = type(inputNumber)
    if type_input is str:
        return inputNumber
    elif type(inputNumber) is int:
        return numberToStr(float(inputNumber))
    elif isinstance(inputNumber, datetime):
        return dateTimeToDateStr(inputNumber)
    elif type(inputNumber) is float:
        if math.isnan(inputNumber):
            return ""
        else:
            numberStr = ""
            divisor = 100000
            for i in range(6):
                numberStr += str(int(inputNumber//divisor))
                inputNumber = inputNumber%divisor
                divisor /= 10
            return numberStr
    elif isinstance(inputNumber, lxml.etree._ElementUnicodeResult):
        return inputNumber.__str__()
    
    else:
        raise Exception("Non defined type {} for {} ".format(type(inputNumber), inputNumber))
        
def numberToDateTime(inputNumber):
    return dateStrToDateTime(numberToStr(inputNumber))

def numberToDateStr(inputNumber):
    return dateTimeToDateStr(dateStrToDateTime(numberToStr(inputNumber)))

def fundDateEleToDateStr(dateEle):
    dateStr = [str(s) for s in dateEle if str(s) != '-']
    return "".join(dateStr)

def rawStockStrToInt(rawStockStr):
    rawStockStr = str(rawStockStr)
    rawStockStr = re.findall(r'\d', rawStockStr)
    rawStockStr = ''.join(rawStockStr)
    return int(rawStockStr)

def roundFloatToCloseDecimal(float_number):
    final_float = round(float_number, 4)#default
    if abs(float_number) > 10:
        final_float = round(float_number, 2)
        return final_float
    if round(float_number,4) == round(float_number, 3):
        final_float = round(float_number, 3)
    if round(float_number, 3) == round(float_number, 2):
        final_float = round(float_number, 2)
    if round(float_number,2) == round(float_number, 1):
        final_float = round(float_number, 1)
    return final_float

def rawTextToNumeric(rawText):
    try:
        rawText = str(rawText)
        if ',' in rawText:
            rawText = rawText.replace(',', '')
        numeric = 0
        if '%' in rawText:
            rawText_new = rawText.replace('%', '')
            numeric = float(rawText_new)
            numeric = round(numeric/100, 4)
        elif '.' in rawText:
            numeric = roundFloatToCloseDecimal(float(rawText))
        else:
            numeric = int(rawText)
    except:
        numeric = rawText
    return numeric
    
def dateTimeToDateStrAuto(dateStr):
    try:
        date = datetime.strptime(dateStr, '%Y-%m-%d %H:%M:%S')
        return dateTimeToDateStr(date)
    except:
        return dateStr

# from SelfTradingSystem.io.database import Database
if os.name == 'nt':
    from SelfTradingSystem.io.excel import excelToDFs
    import sqlite3
    import pandas as pd
    
    def convertShtToDB(xlsx_path):
        db_path = xlsx_path[:-5]+'.db'
        try:
            with sqlite3.connect('Resources.db', isolation_level=None,\
                         timeout=10, check_same_thread=False) as conn:
                df_menu = pd.read_sql_query("SELECT * from " + 'Menu', conn)
            df_dict = excelToDFs(xlsx_path)
            df_dict.update({'Menu': df_menu})
            df_dict.move_to_end('Menu', last=False)
            conn2 = sqlite3.connect(db_path)
            for table, df in df_dict.items():
                df.to_sql(table, con=conn2, if_exists='replace', index=False)
            conn2.commit()
        except:
            print("Cannot create DB {}".format(db_path))
            pass
