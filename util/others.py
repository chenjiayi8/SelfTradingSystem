#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:37:02 2020

@author: frank
"""

import time
import math
from PIL import Image
from SelfTradingSystem.util.extract import extractBetween
from SelfTradingSystem.util.convert import (
    timeStrToDateTime,
    )

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
