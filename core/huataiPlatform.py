# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 11:53:08 2021

@author: Admin
"""

# from pywinauto import mouse
# import pywinauto
import cv2
from PIL import Image, ImageChops, ImageDraw, ImageFont
import time
import os
import sys
import traceback
import numpy as np
from SelfTradingSystem.core.trade import getDFFromDB, printTable
from SelfTradingSystem.util.remindMe import sendEmailBatch as sendEmail
from SelfTradingSystem.util.convert import getTodayDateStr
# from SelfTradingSystem.core.operation import clickOnTarget, getLocation2
from SelfTradingSystem.util.others import  get_host_name
# import pyautogui
import uuid
# from SelfTradingSystem.core.operation import (
#     chopConfirmedTrade
#     )

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
    
def trim2(im):
    if im.mode == 'RGBA':
        im = rgbaTorgb(im)
    im_array = pil_to_cv_image(im)
    im_array_sum = np.sum(im_array, 2)
    im_array_row_sum = np.sum(im_array_sum, 0)
    im_array_row_sum = im_array_row_sum[::-1]
    last_pixel = im_array_row_sum[0]
    im_array_row_sum_bool = im_array_row_sum == last_pixel
    col_num = im_array.shape[1]-list(im_array_row_sum_bool).index(False)+10
    im_array2 = im_array[:,:col_num, :]
    im2 = Image.fromarray(im_array2, 'RGB')
    return im2
    
def strToImg(text, fontSize=12):
    im = Image.new('RGB', (1000, 1000), (255, 255, 255))
    d = ImageDraw.Draw(im)
    font = ImageFont.truetype(font='simkai.ttf', size=fontSize)
    d.text((10,10), text, fill=(0, 0, 0), font=font)
    return trim(im)

def pil_to_cv_image(pil_image):
    open_cv_image = np.array(pil_image) 
    open_cv_image = open_cv_image[:, :, ::-1].copy() 
    return open_cv_image

def getTextMidPoint(img, text='信用', fontSize=8):
    template = strToImg(text, fontSize)
    img = pil_to_cv_image(img)
    template = pil_to_cv_image(template)
    d, w, h = template.shape[::-1]
    res = cv2.matchTemplate(img,template,cv2.TM_CCOEFF)
    min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)
    top_left = max_loc
    return (top_left[0] + round(w/2), top_left[1] + round(h/2))
        
def clickText(Application, text):
    app = Application(backend="uia")
    app.connect(path="D:\\htzqzyb2\\xiadan.exe")
    app_top_window = app.top_window()
    # app_top_window.maximize()
    app_top_window.set_focus()
    time.sleep(2)
    tabCtrl = app_top_window.child_window(auto_id='1001',control_type='Pane')#信用
    tabCtrl_obj = tabCtrl.wrapper_object()
    tabImg = tabCtrl_obj.capture_as_image()
    x, y = getTextMidPoint(tabImg, text=text, fontSize=7)
    app_top_window.set_focus()
    time.sleep(2)
    tabCtrl_obj.click_input(coords=(x,y))
    
def templateMatching(tabImg, text, fontSize):
    from matplotlib import pyplot as plt
    img = pil_to_cv_image(tabImg)
    template = strToImg(text, fontSize)
    template = pil_to_cv_image(template)
    # img2 = img.copy()
    # template = cv2.imread('template.jpg',0)
    d, w, h = template.shape[::-1]
    
    # All the 6 methods for comparison in a list
    methods = ['cv2.TM_CCOEFF', 'cv2.TM_CCOEFF_NORMED', 'cv2.TM_CCORR',
                'cv2.TM_CCORR_NORMED', 'cv2.TM_SQDIFF', 'cv2.TM_SQDIFF_NORMED']
    fig, axs = plt.subplots(len(methods), 2)
    for i in range(len(methods)):
        meth = methods[i]
        img2 = img.copy()
        method = eval(meth)
    
        # Apply template Matching
        res = cv2.matchTemplate(img2,template,method)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)
    
        # If the method is TM_SQDIFF or TM_SQDIFF_NORMED, take minimum
        if method in [cv2.TM_SQDIFF, cv2.TM_SQDIFF_NORMED]:
            top_left = min_loc
        else:
            top_left = max_loc
        bottom_right = (top_left[0] + w, top_left[1] + h)
        cv2.rectangle(img2,top_left, bottom_right, 255, 2)
        # cv2.imwrite(str(i)+".png",img)
        axs[i, 0].imshow(res,cmap = 'gray')
        axs[i, 0].set_title('Matching Result '+meth)
        axs[i, 1].imshow(img2,cmap = 'gray')
        axs[i, 1].set_title('Detected Result '+meth)
  
    fig.show()


def login(Application):
    app = Application(backend="win32")
    app.start("D:\\htzqzyb2\\xiadan.exe")
    app.connect(path="D:\\htzqzyb2\\xiadan.exe")
    # app.Dialog.print_control_identifiers()
    time.sleep(30)
    app_top_window = app.top_window()
    app_top_window.set_focus()
    time.sleep(5)
    app.Dialog['交易密码(&G):Edit2'].wait('visible',timeout=120).type_keys(os.environ['HuataiPass'])
    app.Dialog['通讯密码(&K):Edit'].wait('visible',timeout=120).type_keys(os.environ['HuataiComPass'])
    app.Dialog['确定(&Y)Button'].click()
    time.sleep(60)
    # app.Dialog['Button3'].wait('visible',timeout=120).set_focus().click()#cancel evolution
    # time.sleep(6)
    app.营业部公告Dialog['确定Button'].wait('visible',timeout=120).set_focus().click()
    app_top_window = app.top_window()
    app_top_window.maximize()
    app_top_window.set_focus()
    time.sleep(5)
    # app_top_window_rect_max = app_top_window.rectangle()
    # cash0  = float(app_top_window['Static5'].wait('visible',timeout=30).texts()[0])
    # cash0 += float(app_top_window['Static6'].wait('visible',timeout=30).texts()[0])
    # cash0 = 202.01
    ctrl5 = app_top_window['treeview5']
    ctrl5_obj = ctrl5.wrapper_object()
    ctrl5_obj.ensure_visible('\查询[F4]\资金股票').select()
    time.sleep(5)
    cash0  = float(app_top_window['Static5'].wait('visible',timeout=30).texts()[0])
    cash0 += float(app_top_window['Static6'].wait('visible',timeout=30).texts()[0])
    clickText(Application, '信用')
    # mouse.click(button='left', coords=(73, 1000))#信用
    time.sleep(5)
    
    ctrl5 = app_top_window['treeview5']
    ctrl5_obj = ctrl5.wrapper_object()
    ctrl5_obj.ensure_visible('\查询[F4]\查询资产').select()
    time.sleep(10)#wait the updating of values
    cash = float(app_top_window['Static7'].wait('visible',timeout=30).texts()[0])
    debit = float(app_top_window['Static11'].wait('visible',timeout=30).texts()[0])
    credit = float(app_top_window['Static39'].wait('visible',timeout=30).texts()[0])
    # for i in range(50):
        # print(i, app_top_window['Static'+str(i)].wrapper_object().texts()[0])
    total_value = float(app_top_window['Static3'].wrapper_object().texts()[0])
    total_value -= cash
    total_value = round(total_value, 2)
    credit_cash = cash
    cash += cash0# Normal account
    cash = round(cash, 2)
    ctrl5_obj.item('\查询[F4]').collapse()
    
   
    # 功能已经迁移到下个版本， 无法使用
    # ctrl5_obj.ensure_visible('\其它功能\合约延期').select()
    # # app_top_window.set_focus()
    # time.sleep(5)
    # pyautogui.moveTo(580, 198)#全部选中
    # pyautogui.click()
    # time.sleep(5)
    # pyautogui.moveTo(396, 159)#延期申请
    # pyautogui.click()
    # time.sleep(5)
    # app_top_window = app.top_window()#first popup window
    # # infoText = app_top_window['static0'].wrapper_object().texts()[0]
    # # app_top_window.set_focus()
    # # time.sleep(3)
    # app_top_window['是(&Y)Button'].wait('visible',timeout=30).click()
    # time.sleep(5)
    # # no contract to extend tested
    # app_top_window = app.top_window()#second popup window
    # if app_top_window.rectangle() != app_top_window_rect_max:
    #     app_top_window['确定Button'].wait('visible',timeout=30).click()
    #     time.sleep(3)
    #     app_top_window = app.top_window()
    
    # ctrl5_obj.ensure_visible('\其它功能').expand()
    ctrl5_obj.ensure_visible('\其它功能\预埋单').select()#\预埋单
    time.sleep(2)
    # app_top_window.print_control_identifiers()#inspect
    bid_obj = app_top_window['委托类型ComboBox']
    bid_obj.wait('visible',timeout=30).set_focus().select(2)#0 担保物买入 1 担保物卖出 2 融资买入
    time.sleep(3)
    assert bid_obj.selected_index()==2, 'change code failed'
    # item.click()
    # app_top_window.print_control_identifiers(filename='app_top_window.txt')
    edit_code = app_top_window['证券代码Edit']
    edit_code.wait('visible',timeout=30).set_focus().set_edit_text('162411')
    time.sleep(3)
    assert edit_code.text_block()=='162411', 'enter code failed'
    edit_price =  app_top_window['Edit2']
    edit_price.wait('visible',timeout=30).set_focus().set_edit_text('99.9')
    time.sleep(3)
    assert edit_price.text_block()=='99.9', 'enter price failed'
    edit_amount = app_top_window['单笔数量Edit']
    edit_amount.wait('visible',timeout=30).set_focus().set_edit_text('1000')
    time.sleep(3)
    assert edit_amount.text_block()=='1000', 'enter amount failed'
    button_add = app_top_window['添加Button']
    button_add.wait('visible',timeout=30).set_focus().click()
    time.sleep(3)
    confirmDig = app['提示信息Dialog']
    confirmDig.wait('visible',timeout=30).set_focus().children()[1].click()#0 是, 1 否
    
    button_selectAll = app_top_window['全选Button']
    # button_selectAll.click()
    button_send = app_top_window['发送Button']
    # button_send.click()
    pid = app_top_window.process_id()
    operator = {'bid_obj': bid_obj, 'edit_code': edit_code,
                'edit_price':edit_price, 'edit_amount':edit_amount,
                'button_add': button_add, 'confirmDig': confirmDig,
                'button_selectAll':button_selectAll,
                'button_send':button_send, 'totalValue': total_value,
                'cash':cash, 'debit':debit, 'credit':credit, 'pid': pid,
                'credit_cash':credit_cash,
                }

    return app, operator

def processTasks(sysObj, ordersTable, credit_account):
    tasks = []
    ids = []
    usedCredit = 0
    usedCash = 0
    for i in range(len(ordersTable)):
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
                    money = task['price'] * task['amount']
                    if task['tradeCode'] == 3 :
                        if usedCredit + money > credit_account['credit']*0.99:
                            continue
                        else:
                            usedCredit += money
                    if task['tradeCode'] == 1:
                        if usedCash + money > credit_account['credit_cash']*0.99:
                            continue
                        else:
                            usedCash += money
                    tasks.append(task)
                    ids.append(i)
                else:
                    sysObj.msg.append('手动任务')
                    sysObj.msg.append(str(task))
    orderedTable = ordersTable.loc[ids, :].copy().reset_index(drop=True)
    return sysObj, tasks, usedCredit, usedCash, orderedTable
           
def chop_img_tradedConfirmed(im):
    width, height = im.size
    right = 9*width/10
    im1 = im.crop((0, 0, right, height))
    return im1
  
   
def priceWaitor(Application):
    app = Application(backend="uia")
    app.connect(path="D:\\htzqzyb2\\xiadan.exe")
    app_top_window = app.top_window()
    app_top_window.set_focus()
    time.sleep(2)
    price_waitor = app_top_window.child_window(auto_id='1024',control_type='Image')
    price_waitor = price_waitor.wrapper_object()
    return price_waitor

def killApp(Application, errorFlag=False):
    try:
        app = Application(backend="win32")
        app.connect(path="D:\\htzqzyb2\\xiadan.exe")
        app_top_window = app.top_window()
        app_top_window.set_focus()
        time.sleep(3)
        if errorFlag:
            errorImg = app_top_window.capture_as_image()
            errorPath = os.path.join(os.getcwd(), 'Task{}_error_{}.png'.format(getTodayDateStr(), str(uuid.uuid4())[:6]))
            errorImg.save(errorPath)
        app.kill(soft=False)
    except:
        pass
         
def waitingPrice(price_waitor):
    timeout = 10
    time.sleep(2)
    while timeout > 0:
        timeout -= 1
        time.sleep(1)
        if price_waitor.texts()[0] != '-':
            # print("Current price is {}".format(price_waitor.texts()[0]))
            break
        

def confirmTasks(Application, tasks):
    # tasks = tasks[::-1]
    app = Application(backend="uia")
    app.connect(path="D:\\htzqzyb2\\xiadan.exe")
    app_top_window = app.top_window()
    app_top_window.set_focus()
    time.sleep(2)
    list_obj = app_top_window.child_window(auto_id='1047',control_type='List')
    list_obj = list_obj.wrapper_object()
    img_tradedConfirmed = list_obj.capture_as_image()
    img_tradedConfirmed = chop_img_tradedConfirmed(img_tradedConfirmed)
    tasks_confirmed = list_obj.texts()
    tasks_confirmed = tasks_confirmed[1:]#first is empty
    confirmedFlag = True
    for i in range(len(tasks)):
        task = tasks[i]
        task_confirmed = tasks_confirmed[i]
        if task['code'] != task_confirmed[0]: confirmedFlag = False
        if str(task['tradeCode']) not in task_confirmed[3]: confirmedFlag = False
        if task['price'] != float(task_confirmed[4]): confirmedFlag = False
        if abs(task['amount']) != int(task_confirmed[5]): confirmedFlag = False
        if '成功' not in task_confirmed[7]: confirmedFlag = False
        
    if confirmedFlag:
        exitCode = 0
    else:
        exitCode = 1
    return exitCode, trim(img_tradedConfirmed)

def makeorders(sysObj, app, operator, tasks, killAtExit=True):
    price_waitor = priceWaitor(sysObj.pywinauto_app)
    currentTradeCode = operator['bid_obj'].selected_index()+1
    for task in tasks:
        if currentTradeCode !=  task['tradeCode']:
            operator['bid_obj'].wait('visible',timeout=30).select(task['tradeCode']-1)
            currentTradeCode = task['tradeCode']
        operator['edit_code'].wait('visible',timeout=30).set_focus().set_edit_text(task['code'])
        waitingPrice(price_waitor)
        operator['edit_price'].wait('visible',timeout=30).set_focus().set_edit_text(task['price'])
        time.sleep(2)
        amountStr = "{:.0f}".format(abs(task['amount']))
        operator['edit_amount'].wait('visible',timeout=30).set_focus().set_edit_text(amountStr)
        time.sleep(2)
        operator['button_add'].wait('visible',timeout=30).set_focus().click()
        time.sleep(2)
        for i in range(3):
            time.sleep(2)
            if not app.top_window().is_in_taskbar():
                app['提示信息Dialog'].wait('visible',timeout=30).\
                        set_focus().children()[0].click()#0 是, 1 否
        # if confirmDig.is_visible(): 
        #     confirmDig.set_focus().children()[0].click()#0 是, 1 否
        time.sleep(3)
    operator['button_selectAll'].wait('visible',timeout=30).set_focus().click()
    time.sleep(5)
    operator['button_send'].wait('visible',timeout=30).set_focus().click()
    app['提示信息Dialog'].wait('visible',timeout=30).\
        set_focus().children()[0].click()#0 是, 1 否
    time.sleep(1.8*len(tasks))#wait for submitting tasks
    exitCode, img_tradedConfirmed = confirmTasks(sysObj.pywinauto_app, tasks)
    if exitCode == 0:
        sysObj.successfulTrading = 0
    else:
        sysObj.successfulTrading = 1
    sysObj.imgDict['orderConfirmed'] = img_tradedConfirmed
    if killAtExit and exitCode == 0:
        killApp(sysObj.pywinauto_app)
    return sysObj, exitCode

    

def loginN(Application, loopguard = 5):
    count = 0
    while loopguard > 0:
        try:
            app, operator = login(Application)
            break;
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            trace_back_obj = sys.exc_info()[2]
            traceback.print_tb(trace_back_obj)
            killApp(Application, errorFlag=True)
            count += 1
            print("Failed to login once, tried {} times".format(count))
            loopguard -= 1
            if loopguard <= 0:
                msg = "Failed to login for {} times".format(count)
                sendEmail('Alert from {}'.format(get_host_name()), msg, 'chenjiayi_344@hotmail.com')
                raise(msg)
    return app, operator

def loginAndOrder(sysObj): 
    exitCode = 0
    try:
        app, operator = loginN(sysObj.pywinauto_app)
        sysObj.totalValueFromImg = operator['totalValue']
        # Part 2: processing
        _, orderedTable = getDFFromDB(sysObj.db_sql, 'Ordered', 0, 9)
        orderedTable['TradeCode'] = orderedTable['TradeCode'].apply(float)
        orderedTable['Price'] = orderedTable['Price'].apply(lambda x: round(float(x), 3))
        orderedTable['Amount'] = orderedTable['Amount'].apply(float)
        # use cash at last
        orderedTable = orderedTable.sort_values(by=['TradeCode',  'Amount'], ascending=False).copy().reset_index(drop=True)
        credit_account = {}
        credit_account['debit'] = round(-1*operator['debit'], 2)
        credit_account['credit'] = round(operator['credit'], 2)
        credit_account['credit_cash'] = round(operator['credit_cash'], 2)
        sysObj, tasks, usedCredit, usedCash, orderedTable2  = processTasks(sysObj, orderedTable, credit_account)
        if len(tasks) > 0:
            print("Ready for making orders in ")
            sysObj, exitCode = makeorders(sysObj, app, operator, tasks)
            printTable(orderedTable)
        else:
            print("No need to make orders")
            killApp(sysObj.pywinauto_app)
    
    except (KeyboardInterrupt, SystemExit):
        exitCode = 1
        raise
    except:
        print ("Need assisstance for unexpected error:\n {}".format(sys.exc_info()))
        traceBackObj = sys.exc_info()[2]
        traceback.print_tb(traceBackObj)
        exitCode = 1
        pass
        
    return exitCode, sysObj, operator




if __name__ == "__main__":
    from SelfTradingSystem.core.trade import Trade
    from SelfTradingSystem.io.database import Database
    sql = Database( 'Resources.db')
    sysObj = Trade('本金账本.xlsx', sql, margin_buying_disabled=True)
    # start_time = time.time()
    # app = sysObj.pywinauto_app(backend="win32")
    Application = sysObj.pywinauto_app
    
    # time.sleep(5)
    # app, operator = login(Application)
    # _, orderedTable = getDFFromDB(sysObj.db_sql, 'Ordered', 0, 9)
    # orderedTable['TradeCode'] = orderedTable['TradeCode'].apply(float)
    # orderedTable['Price'] = orderedTable['Price'].apply(float)
    # orderedTable['Amount'] = orderedTable['Amount'].apply(float)
    # sysObj, tasks  = processTasks(sysObj, orderedTable)
    # # exitcode = makeorders(sysObj, app, operator, tasks, killAtExit=False)
    # # exitCode, sysObj = loginAndOrder(sysObj)
    # print(round(time.time()-start_time,2))


    # list_img = list_obj.capture_as_image()
    # list_items = list_obj.items()
    # item = list_items[0]
    # item_item = item.Item()
# descendants = app.top_window().descendants()#control_type="TabControl")

# with open('temp.txt', 'wt') as temp:
#     for i in range(len(descendants)):
#         temp.write('Num {}: {}\n'.format(i, descendants[i].element_info))
    
# tab=pywinauto.controls.hwndwrapper.HwndWrapper(0x00180EBE)
# # tab = app_top_window.child_window(hwnd=)
# for i in range(len(descendants)):
#     temp = getattr(descendants[i], 'menu_item')
#     try:
#         temp('信用')
#         print(i)
#     except:
#         pass
        
# app.top_window().descendants()
# tab =  app_top_window.child_window(found_index=0, class_name="AfxWnd42s")
# tab_obj = tab.wrapper_object()
# print(tab_obj.rectangle())
# for prop in dir(tab_obj):
#     temp = getattr(tab_obj, prop)
#     if callable(temp):
#         try:
#             print(prop+':',temp())
#         except:
#             pass
#     else:
#         print(prop+':', temp)
# print(dir(tab))
# windows = tab.windows()
# treeview0 = app_top_window.child_window(title_re='TreeView0', class_name='SysTreeView32')
# treeview0_obj = treeview0.wrapper_object()
# ctrl1 = app_top_window['treeview1']
# ctrl1_obj = ctrl1.wrapper_object()
# ctrl1_obj2 = ctrl1_obj.ensure_visible('\其它功能')

# ctrl5 = app_top_window['treeview5']
# ctrl5_obj = ctrl5.wrapper_object()
# ctrl5_obj2 = ctrl5_obj.ensure_visible('\其它功能')
# item = ctrl5_obj.get_item('\其它功能')# \信用
# tree_ctrl = item.tree_ctrl# 'control_id': 513,
# item2  = tree_ctrl.ensure_visible('\其它功能')



# item = ctrl5_obj.get_item('\其它功能')
# # ctrl_obj_tree_root = ctrl_obj.tree_root()
# # ctrl_child = ctrl.child_window()



# button = app_top_window.child_window(title_re='修改成本价', class_name='Button')
# button_dlg = button.wrapper_object()
# dlg_check = dlg.check()

# CCustomTabCtrl = app_top_window.child_window(title_re='CCustomTabCtrl2', class_name='CCustomTabCtrl')
# CCustomTabCtrl_obj = CCustomTabCtrl.wrapper_object()
# treeview5 = app_top_window.child_window(title_re='TreeView5', class_name='SysTreeView32')
# treeview5_obj = treeview5.wrapper_object()
# treeview5.GetItem('')
# ctrl = app_top_window['treeview5']
# ctrl0 = app_top_window['treeview']
# ctrl0_obj = ctrl0.wrapper_object()
# ctrl_obj = ctrl.wrapper_object()
# menu = ctrl_obj.menu()
# menu_ctrl = menu.ctrl
# ctrl.GetItem([''])
# ctrl.get_menu_path()
# treeview5_dlg = treeview5.wrapper_object()
# window = app.window()
# child_window = window.child_window()

# app_top_window['确定'].click()
# child_window = app.window(title_re='确定', class_name='Button')
# child_window['Button'].click()


# dlg = child_window.WrapperObject()
# app['网上股票交易系统5.0'].print_control_identifiers(filename='log.txt')
# app_top_window.set_focus()
# for i in range(33):
#     time.sleep(1)
#     print(i)
#     try:
#         app_top_window['Button'+str(i)].draw_outline()#click 信用
#     except:
#         pass
    
# app_top_window.set_focus()
# windows = app.windows()
# for i in range(len(windows)):
#     time.sleep(1)
#     print(i)
#     windows[i].draw_outline()
