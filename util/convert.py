#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:35:58 2020

@author: frank
"""

from datetime import datetime, timedelta
import time
import math
import lxml
import re

def getYearFromDate(dateStr):
    return dateStrToDateStruc(dateStr).tm_year

def getMonthFromDate(dateStr):
    return dateStrToDateStruc(dateStr).tm_mon

def getWeekNumFromDate(dateStr):
    return dateStrToDateTime(dateStr).strftime("%W")

def dateStrToDateTime(dateStr):
    return datetime.strptime(dateStr, "%Y%m%d")

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

def isnumeric(e):
    flag = False
    if e is not None and e != 'None':
        flag = True
    if type(e) == float:
        return not math.isnan(e)
    if type(e) == str:
        try:
            new_e = float(e)
            if not math.isnan(new_e):
                return isnumeric(float(e))
            else:
                flag = False
        except:
            flag = False
    return flag

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
    rawStockStr = re.findall(r'\d', rawStockStr)
    rawStockStr = ''.join(rawStockStr)
    return int(rawStockStr)
    