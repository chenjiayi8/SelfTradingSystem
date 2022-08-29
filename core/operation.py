# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 19:38:12 2019

@author: Frank
"""

# import sys
import autopy
import pyautogui
#import pandas as pd
#import pickle
# import os
import numpy as np
#import math
import time
import random
#import sys
# from OCRForTradingV2 import getFilledResult
# from addLibraries import Helper
# from SelfTradingSystem.util.remindMe import sendEmailBatch as sendEmail
#from TradingSystemV3 import getTodayDateStr
#import pytesseract
from PIL import Image, ImageChops, ImageDraw, ImageFont
import pyperclip
#import tempfile
#import tkinter as tk

        
#currentFolder = os.getcwd();
#orderTableFile = os.path.join(currentFolder, 'ordersTable_PyPickle.out')
#
#if not os.path.isfile(orderTableFile):
#    ordersTable = pd.read_excel('本金账本.xlsx', 'Preorders')
#    f = open(orderTableFile, 'wb')
#    pickle.dump(ordersTable, f)
#    f.close()
#else:
#    f = open(orderTableFile, 'rb')
#    ordersTable = pickle.load(f)
#    f.close()


#def getClipboardText():
#    root = tk.Tk()
#    # keep the window from showing
#    root.withdraw()
#    return root.clipboard_get()

#def getEnteredContent():
#    pyautogui.hotkey('ctrl', 'a')
#    time.sleep(0.2)
#    pyautogui.hotkey('ctrl', 'c')
#    return getClipboardText()



from bokeh.io import webdriver
from bokeh.io.export import get_screenshot_as_png
from bokeh.models import ColumnDataSource, DataTable, TableColumn

def imgToNumber(img):
    import pytesseract
    pytesseract.pytesseract.tesseract_cmd = r'D:\Tesseract_OCR\tesseract.exe'
    custom_config = r'outputbase digits --oem 3 --psm 8'
    result = pytesseract.image_to_string(img, config=custom_config)
    result_number = round(float(result), 2)
    return result_number
    

def countColor(im, color=(255,255,255)):
    if im.mode == 'RGBA':
        im = rgbaTorgb(im)
    x = np.array(im)
    r, g, b = np.rollaxis(x, axis=-1)
    colorMatch  = (r == color[0]).astype(int)
    colorMatch += (g == color[1]).astype(int)
    colorMatch += (b == color[2]).astype(int)
    numMatch = (colorMatch == 3).astype(int).sum()
    return numMatch


def rgbaTorgb(im):
    x = np.array(im)
    r, g, b, a = np.rollaxis(x, axis=-1)
    x = np.dstack([r, g, b])
    return Image.fromarray(x, 'RGB')

def trim(im):
    if im.mode == 'RGBA':
        im = rgbaTorgb(im)
#    bg = Image.new(im.mode, im.size, im.getpixel((0,0)))
    bg = Image.new(im.mode, im.size, im.getpixel((round(im.size[0]/2), im.size[1]-5)))
    diff = ImageChops.difference(im, bg)
    diff = ImageChops.add(diff, diff, 2.0, -100)
    bbox = diff.getbbox()
    if bbox:
        return im.crop(bbox)
    else:
        return im

def dfToImg(df):
    source = ColumnDataSource(df)
#    df_columns = [df.index.name]
    df_columns = df.columns.values
    columns_for_table=[]
    for column in df_columns:
        columns_for_table.append(TableColumn(field=column, title=column))

    data_table = DataTable(source=source, columns=columns_for_table,height=3000,height_policy="max",width_policy="auto",index_position=None)
    web_driver = webdriver.create_firefox_webdriver()
    im = get_screenshot_as_png(data_table, driver=web_driver)
    web_driver.quit()
    return trim(im)
#    export_png(data_table, filename = path)

def strToImg(string):
    im = Image.new('RGB', (1000, 1000), (255, 255, 255))
    d = ImageDraw.Draw(im)
    font = ImageFont.truetype(font='verdana.ttf', size=15)
    d.text((10,10), string, fill=(0, 0, 0), font=font)
    return trim(im)

def getOrderedTaskImg(orderedTable):
    #orderedTable = orderedTable.iloc[::-1]
    targetCols   = orderedTable.columns[[0,1,3,4,5,7,8 ]]
    orderedTable_new = orderedTable.loc[:, targetCols]
    orderedTable_new['Price'] = orderedTable_new['Price'].apply(lambda x: round(x, 3))
    orderedTable_new['PriceDiff'] = orderedTable_new['PriceDiff'].apply(lambda x: '{}%'.format(round(x*100, 2)))
#    fp = tempfile.NamedTemporaryFile(mode="wb", suffix='.png', delete=False)
#    imgpath = os.path.join(os.getcwd(), 'orderedTasks.png')
#    imgpath = fp.name
    im = dfToImg(orderedTable_new)
#    im = Image.open(imgpath)
#    os.remove(imgpath)
#    fp.close()
#    os.unlink(fp.name)
#    fp.delete()
    return im


def getEnteredContent():#TODO: repeat 3 times if failed to get same content
    select_offsetX = 43
    select_offsetY = 169
    copy_offsetX   = 68
    copy_offsetY   = 78
    originPosition = pyautogui.position()
    time.sleep(1)
    pyautogui.rightClick(originPosition)
    time.sleep(2)
    pyautogui.move(select_offsetX, select_offsetY, random.randint(100, 200)/100)
    pyautogui.click()
    time.sleep(2)
    pyautogui.rightClick(originPosition)
    time.sleep(2)
    pyautogui.move(copy_offsetX, copy_offsetY, random.randint(100, 200)/100)
    pyautogui.click()
    return pyperclip.paste()


def numberToStr(inputNumber):
    type_input = type(inputNumber)
    if type_input is str:
        return inputNumber
    elif type(inputNumber) is int:
        return numberToStr(float(inputNumber))
    elif type(inputNumber) is float or isinstance(inputNumber,np.float64) :
        numberStr = ""
        divisor = 100000
        for i in range(6):
            numberStr += str(int(inputNumber//divisor))
            inputNumber = inputNumber%divisor
            divisor /= 10
        return numberStr
    else:
        raise Exception("Non defined type {} for {} ".format(type(inputNumber), inputNumber))

def getLocation():
    time.sleep(2)
    return autopy.mouse.location()

def getLocation2():
    time.sleep(2)
    return pyautogui.position()

def getColorAt(x,y):
    time.sleep(2)
    return autopy.color.hex_to_rgb(autopy.screen.get_color(x,y))

def getLocationAndColor():
    location = getLocation()
    color    = getColorAt(*location)
    print(location)
    print(color)
    return color

def clickOnTarget(x, y):
    time.sleep(0.5)
#    for i in range(2):
    autopy.mouse.smooth_move(x, y)
    autopy.mouse.click(autopy.mouse.Button.LEFT)
    time.sleep(0.1)

def switchTradeMode(task, settings):
    clickOnTarget(settings['xPoint'], settings['FirstPoint'])
    clickOnTarget(settings['xPoint'], settings[task['tradeCode']])

def deleteContent(num):
    for i in range(num):
        autopy.key.tap(autopy.key.Code.BACKSPACE)
        time.sleep(0.005)
    time.sleep(0.1)

def typeCode(task, settings):
#    time.sleep(2)
    clickOnTarget(settings['xPoint'], settings['CodePoint'])
    clickOnTarget(settings['xPoint'], settings['CodePoint'])
    deleteContent(10)
    autopy.key.type_string(task['code'],wpm= random.randint(100, 200))
    time.sleep(0.5)
    backGroundcolor = (236, 243, 246)
    loopGuard = 30
    while loopGuard > 0:
        loopGuard -= 1
        colorAfterTypingCode = getColorAt(684.0, 189.6)
        if colorAfterTypingCode != backGroundcolor:
            break;
        else:
            time.sleep(1)
    copiedStr = getEnteredContent()
    print("Want: {}, get {}".format(task['code'], copiedStr))
    if task['code'] == copiedStr:
        return 0
    else:
        return -1

def typePrice(task, settings):
#    time.sleep(2)
    clickOnTarget(settings['xPoint'], settings['PricePoint'])
    deleteContent(6);
    priceStr = "{:.3f}".format(task['price'])
    autopy.key.type_string(priceStr,wpm= random.randint(50, 100))
    time.sleep(0.2)
    copiedStr = getEnteredContent()
    print("Want: {}, get {}".format(priceStr, copiedStr))
    if priceStr == copiedStr:
        return 0
    else:
        return -1
    
def typeAmount(task, settings):
#    time.sleep(2)
    clickOnTarget(settings['xPoint'], settings['AmountPoint']);
    deleteContent(6);
    amountStr = "{:.0f}".format(abs(task['amount']))
    autopy.key.type_string(amountStr,wpm= random.randint(50, 100))
    time.sleep(0.2)
    copiedStr = getEnteredContent()
    print("Want: {}, get {}".format(amountStr, copiedStr))
    if amountStr == copiedStr:
        return 0
    else:
        return -1

def typeAdd(settings):
    clickOnTarget(settings['xPoint'], settings['AddPoint'])
    time.sleep(0.2)
    
def checkWarning(settings):
    time.sleep(2)
    if getColorAt(704.8, 497.6) == (238, 73, 76):
        clickOnTarget(settings['XWarningPoint'], settings['YWarningPoint'])
#autopy.key.type_string("Hello, world!", wpm=100)

#autopy.mouse.move(461, 168)
#ordersTable = 
def confirmTask(ocrResult, task):
    flag=True
    if task['tradeCode'] != float(ocrResult[0]): flag = False
    if task['code'] != ocrResult[1]: flag = False
    if task['price'] != float(ocrResult[2]): flag = False
    if abs(task['amount']) != float(ocrResult[3]): flag = False
    return flag

def confirmTrade():
    time.sleep(2)
    clickOnTarget(344.8, 297.6)
    time.sleep(1)
    clickOnTarget(445.6, 296.8)
    time.sleep(5)
    clickOnTarget(705.6, 500.0)


def closeTrade(settings):
    clickOnTarget(1528.8, 4.0)
    time.sleep(5)
    checkWarning(settings)

def chopConfirmedTrade():
    time.sleep(2)
    img = pyautogui.screenshot(region=(317, 395, 1081-317, 1001-395)) 
    return img


def isBuying():
#    time.sleep(2)
    color = getColorAt(316.0, 115.2)
    if color[0] > 250:
        return True
    else:
        return False

def isBorrowing():
#    time.sleep(2)
    color = getColorAt(401.6, 114.4)
    if color == (255, 255, 255):
        return True
    else:
        return False
    
def getTradeCode():
    if not isBuying():
        return 2
    else:
        if isBorrowing():
            return 3
        else:
            return 1

def getTotalValueImg():
    clickOnTarget(50.4, 453.6)
    time.sleep(0.2)
    clickOnTarget(65.6, 476.0)
    time.sleep(10)
    img = pyautogui.screenshot(region=(1217, 990, 1345-1217, 1007-990))
    return img
    

def operation(sysObj, ordersTable, availableCredit=[]): #V2 use larger font size
# Part 0: Initialisation
    if type(availableCredit) is list:
        availableCredit = 1e6
    for i in range(5,0, -1):
        print("Open trade windows in {} seconds".format(i))
        time.sleep(1)
    numTasks = len(ordersTable)
    settings = {}
    settings['currentTradeCode'] = -1
    settings['FirstPoint'] = 116.0
    settings['xPoint']  = 385.6
    #TradeCodes = ordersTable['SecondPoint'][0:6]
    settings[1] = 131.2
    settings[2] = 144.8
    settings[3] = 155.2
    settings[4] = 168.0
    settings[5] = 180.8
    settings[6] = 191.2
    settings['CodePoint'] = 135.2
    settings['PricePoint'] = 172.8
    settings['AmountPoint'] = 190.4
    settings['AddPoint'] = 232.8
    settings['XWarningPoint'] = 706.4
    settings['YWarningPoint'] = 499.2  
    
# Part 1: preconditioning
    tasks = []
    for i in range(numTasks):
        if type(ordersTable['Name'][i]) is str:
            task = {}
            task['name'] = ordersTable['Name'][i]
            task['type'] = ordersTable['Remark'][i]
            task['code'] = numberToStr(ordersTable['Code'][i])
            task['tradeCode'] = int(ordersTable['TradeCode'][i])
            task['price'] = round(ordersTable['Price'][i], 3)
            task['amount'] = int(ordersTable['Amount'][i])
            if task['tradeCode'] > 0:
                if task['code'] != '163210':
                    tasks.append(task)
                else:
                    sysObj.msg.append('手动任务')
                    sysObj.msg.append(str(task))

# Part 2: processing
    time.sleep(2)
#    faildTasks = []
    usedCredit = 0
    for task in tasks:
        print("Available credit {}, used credit {}".format(availableCredit, round(usedCredit)))        
        if task['tradeCode'] == 3 and task['amount'] > 0:
            tempMoney = round(task['price']*task['amount'], 2)
            if usedCredit + tempMoney > availableCredit: #does not use tradecode 3 (borrow)
                task['tradeCode'] = 1
            elif task['tradeCode'] == 3:
                usedCredit +=tempMoney
        exitCodes = []
        if task['tradeCode'] != settings['currentTradeCode']:
            switchTradeMode(task, settings)
        settings['currentTradeCode'] = task['tradeCode']
#        print('Switching trade mode done ....\n');
        exitCodes.append(typeCode(task, settings))
#        print('typeCode done ....\n');
        exitCodes.append(typePrice(task, settings))
#        print('typePrice done ....\n');
        exitCodes.append(typeAmount(task, settings))
#        print('typeAmount done ....\n');
        clickOnTarget(394.4, 207.2) #move cursor to other place
        gettedTradeMode = getTradeCode()
        if gettedTradeMode == task['tradeCode']:
            exitCodes.append(1)
        else:
            exitCodes.append(-1)
        print("Want: {}, get {}".format(str(task['tradeCode']), str(gettedTradeMode)))
#        if ocrFlag:
#            ocrResult, img = getFilledResult()
#            confirmed = confirmTask(ocrResult, task)
#        else:
#            confirmed = True
#        status.append(confirmed)
        typeAdd(settings);
#        print('typeAdd done ....\n');   
        checkWarning(settings);
#        print('check Warning done ....\n');
        print("Entered task {}\n".format(task))
        if -1 in exitCodes:
#            faildTasks.append(task)
            sysObj.msg.append('Need assistance in SmartQ')
            sysObj.msg.append(str(task))
#            Helper.sendEmail('Need assistance in SmartQ', str(task), 'chenjiayi_344@hotmail.com')
            return -1, sysObj
#    if len(faildTasks) == 0:
##        falsedTasks = [tasks[code] for code in exitCodes if code == -1]
#        Helper.sendEmail('Need assistance in SmartQ', str(faildTasks), 'chenjiayi_344@hotmail.com')
#        return -1
#    else:
    confirmTrade()
    print("Trading tasks are confirmed\n")
    time.sleep(len(tasks)*1.5+5)

    img_tradedConfirmed = chopConfirmedTrade()
    img_tradedConfirmed = trim(img_tradedConfirmed)
    img_totalValue = getTotalValueImg()
    sysObj.totalValueFromImg = imgToNumber(img_totalValue)
    # orderedTable = sysObj.getOrderedTasks(ordersTable)
    # img_orderedTable = getOrderedTaskImg(orderedTable)
    sysObj.imgDict['totalValue'] = img_totalValue
    # sysObj.imgDict['orderedTable'] = img_orderedTable
    sysObj.imgDict['orderConfirmed'] = img_tradedConfirmed
    # sysObj.imgs += [img_totalValue, img_tradedTable, img_tradedConfirmed]
#    img = mergeImg([img2, img1])

    time.sleep(2)
    # sysObj.msg.append('SmartQ confirmation')
#    sysObj.imgPath = imgPath
#    Helper.sendEmail('SmartQ confirmation', 'All tasks are finished', 'chenjiayi_344@hotmail.com', imgPath)
    closeTrade(settings)
    return 0, sysObj
        



if __name__ == "__main__":
    # img_totalValue = getTotalValueImg()
    from SelfTradingSystem.core.trade import Trade, getDFFromDB
    # from SelfTradingSystem.io.excel import sheetToDF
    # from dateutil.relativedelta import relativedelta
    # from SelfTradingSystem.util.convert import dateTimeToDateStr
    # from SelfTradingSystem.util.others import getLastTradedTime
    from SelfTradingSystem.io.database import Database
    db_path = 'Resources.db'
    sql = Database(db_path)
    sysObj = Trade('本金账本.xlsx', sql)
    _, orderedTable = getDFFromDB(sysObj.db_sql, 'Ordered', 0, 9)
    orderedTable['TradeCode'] = orderedTable['TradeCode'].apply(float)
    orderedTable['Price'] = orderedTable['Price'].apply(float)
    orderedTable['Amount'] = orderedTable['Amount'].apply(float)
    # sysObj.initialSubjects()
    # ordersTable = sheetToDF(sysObj.wb.sheets['Preorders'])
    # targetDate = getLastTradedTime('log.txt')+relativedelta(days=1)
    # ordersTable = sysObj.buildTargetValueTasks(ordersTable, dateTimeToDateStr(targetDate))
    # ordersTable = sysObj.buildMomentumTasks(ordersTable)
    # ordersTable = sysObj.removeInvalidTasks(ordersTable)
    # orderedTable = sysObj.getOrderedTasks(ordersTable)
    # img1 = strToImg('Summary: {}'.format(10000))
    

    # import pandas as pd
    # orderedTable2  = pd.concat([orderedTable,orderedTable,orderedTable,\
    #                             orderedTable,orderedTable,orderedTable,orderedTable])
    # im2 = getOrderedTaskImg(orderedTable2)
#     # import matplotlib.font_manager
#     # fontlist =     matplotlib.font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
#     targetCols   = orderedTable.columns[[0,1,3,4,5,7,8 ]]
#     orderedTable_new = orderedTable.loc[:, targetCols]
#     orderedTable_new['Price'] = orderedTable_new['Price'].apply(lambda x: round(x, 3))
#     orderedTable_new['PriceDiff'] = orderedTable_new['PriceDiff'].apply(lambda x: '{}%'.format(round(x*100, 2)))
#     img2 = dfToImg(orderedTable_new)
#    imgpath = r'D:\Dropbox\For daily life\Investment\orderedTasks.png'
#    save_df_as_image(orderedTable_new, imgpath)

    # exitCode,sysObj = operation(sysObj, ordersTable, 45042.02)

