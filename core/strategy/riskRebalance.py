# -*- coding: utf-8 -*-
"""
Created on Sun Mar  1 09:57:41 2020

@author: Frank
"""
from addLibraries import Helper
from multiprocessing.dummy import Pool as ThreadPool
import xlwings as xw
from datetime import datetime
from TradingSystemV3 import TradeSystem
from TradingSystemV3 import TradeSubject
from TradingSystemV3 import numberToStr
from TradingSystemV3 import indCell
from TradingSystemV3 import getFundHistory, callBatchMethod
from TradingSystemV3 import dateStrToDateTime, getTodayDate, dateTimeToDateStr

class UpdateSubject(TradeSubject):
    def __init__(self, sysObj, i):
        self.name = sysObj.codes[i]
        self.isStock = False
        self.df = []
        self.resetFlag = False
        self.hasNewContent = False
        self.newContents = []
        self.sheetName = sysObj.sheetNames[i]
        self.TCloseStr = '累计净值'
        self.DateStr   = '净值日期'
        sheetName = [s for s in sysObj.sheetNames if name in s]
        if len(sheetName) == 0:
            sysObj.wb.sheets.add(self.sheetName,after=sysObj.wb.sheets[sysObj.sheetNames[-1]])
            self.sht = sysObj.wb.sheets[self.sheetName]   
            UpdateSubject.resetSheet(self, sysObj.pool)
        elif len(sheetName) > 1:
            raise Exception("Too many sheet names {} include {}".format(sheetName, name))
        else:
            self.sht = sysObj.wb.sheets[self.sheetName]  
            
        numLastRow = self.sht.range('M1').current_region.last_cell.row
        if numLastRow < 3:
            UpdateSubject.resetSheet(self, sysObj.pool)
            numLastRow = self.sht.range('M1').current_region.last_cell.row
            self.lastUpdateDateStr = numberToStr(self.sht.range(numLastRow, 1).value)
        else:
            self.lastUpdateDateStr = numberToStr(self.sht.range(numLastRow, 1).value)
            
    def writeUpdatedSheet(self):
        if self.resetFlag:
            self.sht.range('A1').options(index=False).value = self.df
            self.resetFlag = False
        elif self.hasNewContent:
            numLastRow = self.sht.range('A1').current_region.last_cell.row
            self.sht.range(numLastRow+1, 1).value = self.newContents
            self.newContents = []
            self.hasNewContent = False
    

    @staticmethod                         
    def updateSheet(subObj, pool=[]):
        if subObj.resetFlag:
            subObj.sht.clear_contents()
            if subObj.isStock:
                subObj.df = getStockHistory(subObj.name, pool=pool)
            else:
                subObj.df = getFundHistory(subObj.name, pool=pool)
            subObj.df.loc[:, subObj.DateStr] = subObj.df.loc[:, subObj.DateStr].apply(numberToDateStr)
#            subObj.df = subObj.df.iloc[::-1].copy().reset_index(drop=True)
#            subObj.sht.range('A1').options(index=False).value = subObj.df
        else:
            startDate=dateStrToDateTime(subObj.lastUpdateDateStr)  
            if startDate < getTodayDate():
                if subObj.isStock:
                     sht_new_df = getStockHistory(subObj.name, startDate=startDate, pool=pool)
                else:
                     diffDays = (getTodayDate() - startDate).days
                     sht_new_df = getFundHistory(subObj.name, rows=diffDays, pool=pool)
                sht_appended = sht_new_df[sht_new_df[subObj.DateStr].map(dateStrToDateTime) > startDate].copy()
#                sht_appended.loc[:, self.DateStr] = sht_appended.loc[:, self.DateStr].apply(numberToDateStr)
                sht_appended[subObj.DateStr] = sht_appended[subObj.DateStr].apply(numberToDateStr)
#                sht_appended = sht_appended[sht_appended[self.DateStr].apply(numberToDateStr)]
#                self.df = pd.concat([self.df, sht_appended], sort=False)
#                self.df = self.df.reset_index()
                subObj.newContents = sht_appended.values.tolist()
                subObj.hasNewContent = True
#                subObj.sht.range(numLastRow+1, 1).value = 
    
    @staticmethod 
    def resetSheet(subObj, pool=[]):
        subObj.resetFlag = True
        TradeSubject.updateSheet(subObj, pool)
        subObj.writeUpdatedSheet()      


class UpdateSystem(TradeSystem):
    def __init__(self, xlsxName):
        self.wb         = xw.Book(xlsxName)
        self.pool       = ThreadPool(20);
        self.sht_menu   = self.wb.sheets['列表']
        self.codes      = self.sht_menu.range('K2', 'Q2').value
        self.sheetNames = self.sht_menu.range('K1', 'Q1').value
        self.codeStrs   = [numberToStr(c) for c in self.codes]
        

    def UpdateSubjects(self):
        sheetNames = [self.wb.sheets[s].name for s in range(self.wb.sheets.count)]
        for i in range(len(self.codes)):
            temp_codeStr   = self.codeStrs[i]
            temp_sheetName = self.sheetNames[i]
            if temp_sheetName not in sheetNames:
                self.wb.sheets.add(temp_sheetName,after=self.wb.sheets[self.sheetNames[-1]])
            temp_sht = self.wb.sheets[temp_sheetName]
            numLastRow = temp_sht.range('M1').current_region.last_cell.row
            if numLastRow < 3:
                temp_df  = getFundHistory(temp_codeStr, rows=0, pool=self.pool)#(lambda x : datetime.strftime(dateStrToDateTime(x), "%d/%m/%Y"))
                temp_df.loc[:, '净值日期'] = temp_df.loc[:, '净值日期'].apply(lambda x : dateStrToDateTime(x))
                temp_sht.range('M1').options(index=False).value = temp_df
            else:
                startDate = temp_sht.range(indCell('M', numLastRow)).value
                diffDays = (getTodayDate() - startDate).days
                if diffDays > 0:
                    sht_new_df = getFundHistory(temp_codeStr, rows=diffDays, pool=self.pool)
                    sht_appended = sht_new_df[sht_new_df['净值日期'].map(dateStrToDateTime) > startDate].copy()
                    sht_appended['净值日期'] = sht_appended['净值日期'].apply(lambda x : dateStrToDateTime(x))
                    newContents = sht_appended.values.tolist()
                    temp_sht.range(indCell('M', numLastRow+1)).value = newContents
                
    def calculate(self):
        self.wb.app.calculate()
    
    def save(self):
        self.wb.save()
    
    def checkAndSendReminder(self):
        lastTradedDate = self.sht_menu.range('H13').value
        message = "在 {} 调平".format(datetime.strftime(lastTradedDate, "%d/%m/%Y"))
        diffDays = (getTodayDate() - lastTradedDate).days
        if diffDays < 5:
            Helper.sendEmail('风险平价', message, 'chenjiayi_344@hotmail.com')
    def close(self):
        self.wb.app.kill()
            
            

def runRoutine():
    xlsxName = 'A股ETF分析v4.2风险平价周调版USING.xlsx'
    sysObj = UpdateSystem(xlsxName)
    sysObj.batchMethods = {}
    sysObj.batchMethods['UpdateSubjects'] = sysObj.UpdateSubjects
#    sysObj.UpdateSubjects()
    callBatchMethod(sysObj, 'UpdateSubjects')
    sysObj.calculate()
    sysObj.save()
    sysObj.checkAndSendReminder()
    sysObj.close()
#        sheetNames = [ obj.sheetName for obj in self.tradeObjs]
#        self.objMap = dict(zip(sheetNames, self.tradeObjs))
#        self.sheetsToDFs()

if __name__ == '__main__':
    runRoutine()