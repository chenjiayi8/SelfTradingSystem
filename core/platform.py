# -*- coding: utf-8 -*-
"""
Created on Wed Dec 11 08:30:08 2019

@author: Frank
"""

import autopy
import pyautogui
import time
import subprocess
import random
import math
import numpy as np
from scipy.ndimage.measurements import label


def smoothly_move_mouse(dst_x, dst_y):
    '''
    Smoothly moves the cursor to the given (x, y) coordinate in a
    straight line.
    '''
    width, length = autopy.screen.size()
    if not (width > dst_x and length > dst_y and dst_y > 0 and dst_x):
        raise ValueError("Point out of bounds")
        return

    x, y = autopy.mouse.location()
    velo_x = velo_y = 0.0
#    initial_distance = math.hypot(x - dst_x, y - dst_y)
    while True:
        distance = math.hypot(x - dst_x, y - dst_y)
        if distance <= 1.0:
            break

        gravity = random.uniform(0.0001, 0.0005)
#        velo_x += (gravity * (dst_x - x)) / distance
#        velo_y += (gravity * (dst_y - y)) / distance
        velo_x = gravity
        velo_y = gravity
        

        # Normalize velocity to get a unit vector of length 1.
        velo_distance = math.hypot(velo_x, velo_y)
        velo_x /= velo_distance
        velo_y /= velo_distance

        x += int(round(velo_x))
        y += int(round(velo_y))

        autopy.mouse.move(x, y) # Automatically raises an exception if point
                                # is out of bounds.

        time.sleep(random.uniform(0.001, 0.003))


def getLocation():
#    time.sleep(2)
    return autopy.mouse.location()

def getColorAt(x,y):
    time.sleep(2)
    return autopy.color.hex_to_rgb(autopy.screen.get_color(x,y))

def getLocationAndColor():
    time.sleep(3)
    location = getLocation()
    color    = getColorAt(*location)
    print(location)
    print(color)
    return color


def checkCondition1():
    color = getColorAt(844.8, 397.6)
    return color == (236, 243, 246)

def checkCondition2():
    color = getColorAt(696.0, 322.4)
    return color == (255, 255, 255)

def checkCondition3():
    color = getColorAt(789.6, 408.8)
    return color == (236, 243, 246)

def clickOnTarget(x, y):
#    time.sleep(2)
#    for i in range(2):
#    smoothly_move_mouse(x, y)
    autopy.mouse.smooth_move(x, y)
#    pyautogui.click(x,y, duration=1)  
    autopy.mouse.click(autopy.mouse.Button.LEFT)
    time.sleep(0.1)

def getNewsAcceptLocation():
    x1 = 526
    y1 = 175
    x2 = 1396
    y2 = 862
    
    time.sleep(2)
    im = pyautogui.screenshot(region=(x1, y1, x2-x1, y2-y1))
#    im = Image.open('Temp.png')
    
    img2 = im.convert('L')
    img2_array = np.asarray(img2).copy()
    img2_array_bool = img2_array == 255
    img2_array_bool_int = img2_array_bool.astype(int)
    
    structure = np.ones((3, 3), dtype=np.int)
    labeled, ncomponents = label(img2_array_bool_int, structure)
    area_components = []
    area_components_id = []
    for i in range(1, ncomponents+1):
        area_components.append(np.count_nonzero(labeled == i))
        area_components_id.append(i)
        
    area_components   = np.array(area_components)    
    area_components_id = np.array(area_components_id)
    final_areas_id = area_components_id[np.where(np.logical_and(area_components>=100, area_components<=200))]    
    area_map = np.zeros_like(img2_array)
    
    for i in range(len(final_areas_id)):
        area_id= final_areas_id[i]
        area_map[labeled==area_id] = i+1
        
    
    
    idx_ys, idx_xs = np.where(area_map != 0)
    quarter_y_top = round(len(img2_array)*3/4) # they are at lower part of image
    quarter_y_bottom = round(len(img2_array)*9/10) # they are at lower part of image
    idx_xs = idx_xs[(idx_ys > quarter_y_top) & (idx_ys < quarter_y_bottom)]
    idx_ys = idx_ys[(idx_ys > quarter_y_top) & (idx_ys < quarter_y_bottom)]

    
    count = len(idx_ys)
    addY = int(round(sum(idx_ys)/count))+y1
    addX = int(round(sum(idx_xs)/count))+61+x1
    return addX, addY    
    
def checkCondition4():
    loopGuard = 10
    while loopGuard > 0:
        loopGuard -= 1
        time.sleep(0.5)
        screen_shot = autopy.bitmap.capture_screen()
        if screen_shot.count_of_color((236, 243, 246)) > 140000:
            print("acceptNews2 done")
            break

def acceptNews():
    loopGuard = 10
    while loopGuard > 0:
        loopGuard -= 1
        time.sleep(0.5)
        if getColorAt(755.2, 631.2) ==  (110, 135, 144):
            clickOnTarget(755.2, 631.2)
            break
    if loopGuard <= 0 :
        clickOnTarget(755.2, 631.2)
        
        
def acceptNews2():
    time.sleep(random.randint(20, 40))
    x, y = getNewsAcceptLocation()
    # pyautogui.move(x, y,0.5)
    pyautogui.click(x,y, duration=1)       

def maximizeWindow():
    time.sleep(2)    
    autopy.key.tap(autopy.key.Code.SPACE, [autopy.key.Modifier.ALT])
    time.sleep(0.2)
    autopy.key.tap("X")

def checkMarginTradingPage():
    time.sleep(2)
    loopGuard = 10
    while loopGuard > 0:
        loopGuard -= 1
        time.sleep(0.5)
        if getColorAt(1045.6, 226.4) != (255, 255, 255):
            print("Get margin trading page")
            break

def checkCondition6():
    color = getColorAt(623.2, 444.0)
    return color == (7, 9, 43)

def login():
    subprocess.Popen(["C:\\htzqzyb2\\xiadan.exe"])
    loopGuard = 20
    while loopGuard > 0:
        loopGuard -= 1
        time.sleep(1)
        if checkCondition1() and checkCondition2() and checkCondition3():
            print("Trading system window is opened\n")
            break
        
    clickOnTarget(745.6, 320.8)
    autopy.key.type_string('XXXX',wpm= random.randint(50, 100))
    clickOnTarget(691.2, 356.0)
    autopy.key.type_string('XXXX',wpm= random.randint(60, 90))
    clickOnTarget(907.2, 235.2)



#loopGuard = 10
#while loopGuard > 0:
#    loopGuard -= 1
#    time.sleep(0.5)
#    if checkCondition1() and checkCondition2() and checkCondition3():
#        print("Trading system window is opened\n")
#        break
def postProcess():
    acceptNews2()
    checkCondition4()
    maximizeWindow()
    time.sleep(1)
    clickOnTarget(55.2, 792.0) #信用
    #time.sleep(1)
    checkMarginTradingPage()
    #time.sleep(3)
    clickOnTarget(48.0, 474.4) #其他功能 
    time.sleep(0.5)
    clickOnTarget(66.4, 595.2) #预埋单 
    print("Reach target window")
    
    
login()
postProcess()
#time.sleep(2)
#time.sleep(1)
#from SmartQ_Python import SmartQ_Python
#ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
#SmartQ_Python(ordersTable)

