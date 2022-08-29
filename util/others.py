#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:37:02 2020

@author: frank
"""
import platform
import time
import math
from PIL import Image
from SelfTradingSystem.util.extract import extractBetween
from SelfTradingSystem.util.convert import (
    timeStrToDateTime,
    )

import matplotlib
import matplotlib.markers
from matplotlib.colors import CSS4_COLORS as colors
import random
import os
import pickle
import uuid

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

def isSameDate(this, that):
    thisDict = datetimeToDict(this)
    thatDict = datetimeToDict(that)
    if thisDict['Year'] != thatDict['Year']: return False
    if thisDict['Month'] != thatDict['Month']: return False
    if thisDict['Day'] != thatDict['Day']: return False
    return True

def debugger(objList, filename):
    resultFile = os.path.join(os.getcwd(), '{}_{}.pickle'.format(filename, str(uuid.uuid4())[:6]))
    if os.path.isfile(resultFile):
        os.remove(resultFile)
    f = open(resultFile, 'wb')
    pickle.dump(objList, f)
    f.close()
    
def readBug(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

def get_host_name():
    return platform.uname()[1]

def mergeImg(images):
    widths, heights = zip(*(i.size for i in images))
    total_height = sum(heights)
    max_width = max(widths)
    new_im = Image.new('RGB', (max_width, total_height), color=(255,255,255))
    y_offset = 0
    for im in images:
      new_im.paste(im, (0,y_offset))
      y_offset += im.size[1]
    return new_im


def sleep(seconds=0, mins=0, hours=0, days=0): #for KeyboardInterrupt 
    total_seconds = seconds + mins*60 + hours*60*60 + days*24*60*60
    for i in range(int(total_seconds*10)):
        time.sleep(0.1)
        
        
        
def getLastTradedTime(log_path):
    with open(log_path, 'rt') as log:
        lines = log.readlines()
    if len(lines) == 0:
        raise Exception("No traded time found")
    else:
        lines = [line for line in lines if 'Success' in line]
        lastLine = lines[-1]
        lastTradedTimeStr = extractBetween(lastLine, 'Success at ', '\n')[0]
        lastTradedTime    = timeStrToDateTime(lastTradedTimeStr)
        return lastTradedTime
    
    
    

def round_up(n, decimals=0): 
    multiplier = 10 ** decimals
    if multiplier > 0:
        return math.ceil(n * multiplier) / multiplier
    else:
        return math.floor(n * multiplier) / multiplier

def round_down(n, decimals=0): 
    multiplier = 10 ** decimals 
    if multiplier > 0:
        return math.ceil(n * multiplier) / multiplier
    else:
        return math.floor(n * multiplier) / multiplier
    
    
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



markers =  matplotlib.markers.MarkerStyle.markers 
markers = [m for m in markers.keys() if m is not None and m != 'None']
colors = [c for c in colors.keys() if c is not None and c != 'None']
preOrderMarkers = ['o', 'x', '+', 'P', 'D', 's', 2, 3, '3', '4']
preOrderColors = ['black', 'red', 'blue', 'green', 'cyan', 'purple', 'yellow', 'orange', 'cyan', 'pink']

randomSeed=23
def getRandomMarkers(numMarker, usingPreordered=True, usingColor=False):
    if numMarker > len(preOrderMarkers):
        usingPreordered = False
    if usingPreordered:
        random.seed(randomSeed)
        randomMarkers  = random.sample(preOrderMarkers, numMarker)
        randomColors  = random.sample(preOrderColors, numMarker)  
    else:
        random.seed()
        randomMarkers = random.sample(markers, numMarker)
        randomColors  = random.sample(colors, numMarker)
     
    if not usingColor:
        randomColors = []
    return randomMarkers, randomColors

def xirr(transactions):
    years = [(ta[0] - transactions[0][0]).days / 365.0 for ta in transactions]
    residual = 1
    step = 0.05
    guess = 0.05
    epsilon = 0.0001
    limit = 10000
    while abs(residual) > epsilon and limit > 0:
        limit -= 1
        residual = 0.0
        for i, ta in enumerate(transactions):
            residual += ta[1] / pow(guess, years[i])
        if abs(residual) > epsilon:
            if residual > 0:
                guess += step
            else:
                guess -= step
                step /= 2.0
    return guess-1

if __name__ == '__main__':
    from datetime import date
    tas = [ (date(2010, 12, 29), -10000),
        (date(2012, 1, 25), 20),
        (date(2012, 3, 8), 10100)]
    print(xirr(tas)) #0.0100612640381