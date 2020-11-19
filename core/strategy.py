# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 14:55:45 2020

@author: Frank
"""

import numpy as np
import pandas as pd
import pdpipe as pdp
from SelfTradingSystem.io.subject import Subject, updateRelativeMomentumWrapper
from SelfTradingSystem.util.stock import (
    getStock, getFund, getStocks, getHTML
    )
from SelfTradingSystem.util.others import (
    round_up, round_down, getLastTradedTime,
    isnumeric, mergeImg, sleep
    )
from SelfTradingSystem.io.excel import (
    sheetToDF, indCell, getColumnStr, getTargetArea
    )     

from SelfTradingSystem.util.convert import (
    numberToStr, numberToDateStr, dateStrToDateTime,
    getTodayDate, getWeekNumFromDate, getMonthFromDate,
    getYearFromDate, rawStockStrToInt, rawTextToNumeric,
    getStockNumberStr, dateTimeToDateStr, getTomorrowDateStr,
    getTodayDateStr, getDeltaDateStr, getNowTimeStr,
    getDaysBetweenDateStrs, getTomorrowDate
    )


def getTradingPlanAS(df_row, modifier):
    ind = df_row.index[0]
    openning_price = df_row.loc[ind, '开网价格']*modifier #I2
    openning_position = df_row.loc[ind, '开网金额'] #J2
    net_interval = df_row.loc[ind, '网眼大小']     #K2
    openning_net_num = df_row.loc[ind, '开网网数'] #L2
    position_modifier = df_row.loc[ind, '金额倍数'] #M2
    profit_modifier = df_row.loc[ind, '留利润倍数'] #N2
    max_net_num = int(round(1/net_interval))
    columns = ['net_num', 'buy_price', 'buy_position', 'buy_share', 'sell_price', 'sell_position', 'sell_share']
    tradingPlan = np.zeros([max_net_num, 7], dtype=np.float)#buy_price, buy_position, buy_share, sell_price, sell_position, sell_share
    for i in range(max_net_num):
        tradingPlan[i][0] = i+1
        tradingPlan[i][1] = round((openning_price/(1-net_interval*openning_net_num))*(1-net_interval*(i+1)), 3)
        tradingPlan[i][2] = round(openning_position*(1+position_modifier)**i, 2)
        if tradingPlan[i][1] > 0:
            tradingPlan[i][3] = round_up(tradingPlan[i][2]/tradingPlan[i][1], -2)
        tradingPlan[i][4] =  round((openning_price/(1-net_interval*openning_net_num))*(1-net_interval*(i-1)), 3)
        tradingPlan[i][5] = round(-1*(openning_position*(1+position_modifier)**(i-1))*(1-net_interval*profit_modifier), 2)
        if tradingPlan[i][4] > 0:
            tradingPlan[i][6] = round_down(tradingPlan[i][5]/tradingPlan[i][4], -2)
            
    df_plan = pd.DataFrame(data=tradingPlan, columns=columns)      
    return df_plan

def getTradingPlanDefault(df_row, modifier):
    ind = df_row.index[0]
    openning_price = df_row.loc[ind, '开网价格']*modifier #I2
    openning_position = df_row.loc[ind, '开网金额'] #J2
    net_interval = df_row.loc[ind, '网眼大小']     #K2
    openning_net_num = df_row.loc[ind, '开网网数'] #L2
    position_modifier = df_row.loc[ind, '金额倍数'] #M2
    profit_modifier = df_row.loc[ind, '留利润倍数'] #N2
    max_net_num = int(round(1/net_interval))
    columns = ['net_num', 'buy_price', 'buy_position', 'buy_share', 'sell_price', 'sell_position', 'sell_share']
    tradingPlan = np.zeros([max_net_num, 7], dtype=np.float)#buy_price, buy_position, buy_share, sell_price, sell_position, sell_share
    for i in range(max_net_num):
        tradingPlan[i][0] = i+1
        tradingPlan[i][1] = round(openning_price*(1+net_interval*(-i+openning_net_num-1)), 3)
        tradingPlan[i][2] = round((openning_position*(1+position_modifier)**i), 2)
        if tradingPlan[i][1] > 0:
            tradingPlan[i][3] = round_up(tradingPlan[i][2]/tradingPlan[i][1], -2)
        tradingPlan[i][4] =  round(openning_price*(1+net_interval*(1-i+openning_net_num)), 3)
        tradingPlan[i][5] = round(-1*(openning_position*(1+position_modifier)**(i-1))*(1-net_interval*profit_modifier), 2)
        if tradingPlan[i][4] > 0:
            tradingPlan[i][6] = round_down(tradingPlan[i][5]/tradingPlan[i][4], -2)
            
    df_plan = pd.DataFrame(data=tradingPlan, columns=columns)      
    return df_plan


def getTradingPlanMain(df_row,netType, modifier):
    if netType == '等差':
        return getTradingPlanAS(df_row, modifier)
    elif netType == '初始':
        return getTradingPlanDefault(df_row, modifier)

def getTargetValuePlan(df_row, momentumTargets):
    ind = df_row.index[0]
    name          = df_row.loc[ind, '基金名称']
#    tomorrow_date = getTomorrowDateStr()
    current_price =  df_row.loc[ind, '当前价格']
#    initial_price = float(df_row.loc[ind, '期初价格'])
    current_share = float(df_row.loc[ind, '持有份数'])
    target_value = df_row.loc[ind, '目标市值'] #I2
#    target_date = dateTimeToDateStr(df_row.loc[ind, '下期时间'])
    tempTradedStr = df_row.loc[ind, '临时操作记录']
    tempTradedSyms = tempTradedStr.split('/')
    tempSold   = tempTradedSyms[0] != '0'
    tempBought = tempTradedSyms[1] != '0'
    momentum_value = 0.0
    momentum_share = 0
    momentum_price = 0.000
    if '周' in name:
        return  momentum_share, momentum_value, momentum_price
    else:
        isLockBuy  = any(target in name for target in momentumTargets[0])
        isLockSell = any(target in name for target in momentumTargets[1])
        current_value = current_share*current_price
        if current_value > target_value * 1.20 and not tempSold and isLockSell:
            momentum_price = current_price#round(target_value*1.20/current_share, 3)
            momentum_share = -1*round_down((target_value*1.20-target_value)/momentum_price, -2)
        elif current_value < target_value*0.85 and not tempBought and isLockBuy:
            momentum_price = current_price#round(target_value*0.85/current_share, 3)
            momentum_share = round_down((target_value - target_value*0.85)/current_price, -2)
            momentum_value = momentum_price*momentum_share
        return  momentum_share, momentum_value, momentum_price


def calculateForMomentumShareForNormal(wb, header, momentumTargets):
    sheetName = '普通网格'
    targetsList = momentumTargets[0] + momentumTargets[1]
    sht = wb.sheets['Preorders']
    df  = getTargetArea(sht, 'A', 'I')
    df = df[df['Remark'] == sheetName].copy().reset_index(drop=True)
    numRows = len(df)
    df_latestStockPrices = sheetToDF(wb.sheets['股票查询'])
    pipeline  = pdp.ApplyByCols('股票代码', getStockNumberStr, '股票代码', drop=False)
    df_latestStockPrices = pipeline(df_latestStockPrices)
    data = []
    for i in range(numRows):
        df_row = df.iloc[[i]].copy()
        ind = df_row.index[0]
        name   = df_row.loc[ind, 'Name']
        if any(target in name for target in targetsList):
            code   = numberToStr(df_row.loc[ind, 'Code'])
            date   = getTodayDate()
            price  = df_row.loc[ind, 'Price']
            amount = df_row.loc[ind, 'Amount']
            lockStatus = df_row.loc[ind, '锁仓']
            momentum_share = 0
            momentum_value = 0.0
            momentum_price = 0.000
            if len(lockStatus) > 0 and amount != 0:
                targetPrice = round(price, 3)
                momentum_share = amount
                price_open  = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == code, '今开'].values[0]
                price_max   = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == code, '最高'].values[0]
                price_min   = df_latestStockPrices.loc[df_latestStockPrices['股票代码'] == code, '最低'].values[0]
                if amount > 0:
                    if targetPrice >= price_min:
                        if targetPrice > price_open:
                            momentum_price = price_open
                        else:
                            momentum_price = targetPrice
                else: #amount != 0 
                    if targetPrice <= price_max:
                        if targetPrice < price_open:
                            momentum_price = price_open
                        else:
                            momentum_price = targetPrice
                if '卖' in lockStatus:
                    momentum_value = 0.0
                else:
                    momentum_value = round(price*amount, 2)
                data.append([name, code, date, momentum_share, momentum_price, momentum_value,sheetName])    
    task  = pd.DataFrame(data=data, columns=header)
    return task

def calculateForMomentumShare(wb,sheetName, header, momentumTargets):
    targetsList = momentumTargets[0] + momentumTargets[1]
    if sheetName == '普通网格':
        return calculateForMomentumShareForNormal(wb, header, momentumTargets)
    else:
        sht = wb.sheets[sheetName]
        df  = sheetToDF(sht)
        numRows = len(df)
        data = []
        for i in range(numRows):
            df_row = df.iloc[[i]].copy()
            ind = df_row.index[0]
            name   = df_row.loc[ind, '基金名称']
            if name == '全指医药ETF5':
                sleep(0.1)
            if any(target in name for target in targetsList):
                code   = numberToStr(df_row.loc[ind, '基金代码'])
                date   = df_row.loc[ind, '净值日期']
                momentum_share = 0
                momentum_value = 0.0
                momentum_price = 0.000
                if '网格' not in sheetName:
                    momentum_share, momentum_value, momentum_price = getTargetValuePlan(df_row, momentumTargets)
                else:
                    if sheetName == '简易网格':
                        netType = '等差'
                        modifier = df_row.loc[ind, '调整因子']
                    else:
                        netType = df.loc[i, '网格类型']
                        modifier = 1
                    df_plan = getTradingPlanMain(df_row,netType,modifier)
                    net_interval = df.loc[i, '网眼大小']
                    if net_interval < 0.15:
                        current_net_num = int(df.loc[i, '持有网数'])
                        current_price =  df.loc[i, '当前价格']
                        current_price_row_idx = (df_plan['buy_price']<=current_price) & (df_plan['sell_price']>=current_price)
                        if True in list(current_price_row_idx):
                            current_price_row = df_plan[current_price_row_idx].index[0]
                            target_net_num = df_plan.loc[current_price_row, 'net_num']
                        else:
                            current_price_row = -1
                            target_net_num = 0
                        target_idxs = df_plan.loc[df_plan['net_num']==current_net_num].index
                        if len(target_idxs) > 0: 
                            current_net_row = target_idxs[0]
                            if current_net_num < target_net_num: #need buy more
                                buy_postions = sum(df_plan.loc[current_net_row+1:current_price_row-1, 'buy_position'])
                                if buy_postions > 0:
                                    momentum_price = current_price
                                    momentum_share = round(buy_postions/current_price, -2)
                                    momentum_value = momentum_price*momentum_share
                            elif current_net_num > target_net_num: #need sell more
                                if target_net_num != 0:
                                    sell_shares = sum(df_plan.loc[current_price_row+1:current_net_row, 'sell_share'])
                                else:
                                    sell_shares = sum(df_plan.loc[0:current_net_row, 'sell_share'])
                                if sell_shares < 0:
                                    momentum_price = current_price
                                    momentum_share = sell_shares
                data.append([name, code, date, momentum_share, momentum_price, momentum_value, sheetName])
        task  = pd.DataFrame(data=data, columns=header)
        return task



