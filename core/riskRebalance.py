# -*- coding: utf-8 -*-
"""
Created on Sun Mar  1 09:57:41 2020

@author: Frank
"""

import xlwings as xw
import os 
from datetime import datetime
from SelfTradingSystem.io.database import Database
from SelfTradingSystem.util.remindMe import sendEmailBatch as sendEmail
from SelfTradingSystem.util.convert import (
    numberToStr, dateStrToDateTime, getTodayDate
    )
from SelfTradingSystem.io.excel import indCell


class UpdateSystem():
    def __init__(self, xlsxName, sql):
        self.wb         = xw.Book(xlsxName)
        self.sql        = sql
        self.sht_menu   = self.wb.sheets['列表']
        codes      = self.sht_menu.range('K2', 'Q2').value
        self.sheetNames = self.sht_menu.range('K1', 'Q1').value
        self.codeStrs   = [numberToStr(c) for c in codes]
        

    def UpdateSubjects(self):
        for i in range(len(self.sheetNames)):
            subjectname  = 'F' + self.codeStrs[i]
            sheetName = self.sheetNames[i]
            temp_sht = self.wb.sheets[sheetName]
            numLastRow = temp_sht.range('M1').current_region.last_cell.row
            startDate = temp_sht.range(indCell('M', numLastRow)).value
            diffDays = (getTodayDate() - startDate).days
            if diffDays > 0:
                df = self.sql.getLastRows(subjectname, diffDays+10)
                sht_appended = df[df['净值日期'].map(dateStrToDateTime) > startDate].copy()
                sht_appended['净值日期'] = sht_appended['净值日期'].apply(lambda x : dateStrToDateTime(x))
                columns = temp_sht.range('M1:S1').value
                sht_appended = sht_appended[columns]
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
            sendEmail('风险平价', message, 'chenjiayi_344@hotmail.com')
            
    def close(self):
        # self.wb.app.kill()
        if len(self.wb.app.books) != 1:
           self.wb.close()
        # close excel application if only one workbook is open
        else:
            excel_app = xw.apps.active
            excel_app.quit()
            
            

def runRoutine():
    xlsxName = 'A股ETF分析v4.2风险平价周调版USING.xlsx'
    if datetime.fromtimestamp(os.path.getmtime(xlsxName)) < getTodayDate():
        db_path = 'Resources.db'
        sql = Database(db_path)
        sysObj = UpdateSystem(xlsxName, sql)
        sysObj.UpdateSubjects()
        # for i in range(len(sysObj.sheetNames)):
        #     subjectname  = 'F' + sysObj.codeStrs[i]
        #     if subjectname not in sql.subjectnames:
        #         print("{} is not in the database".format(subjectname))
        #         print("Trying to insert {}".format(subjectname))
        #         sql.insertSubject(subjectname)
        sysObj.calculate()
        sysObj.save()
        sysObj.checkAndSendReminder()
        sysObj.close()

if __name__ == '__main__':
    pass
    # runRoutine()