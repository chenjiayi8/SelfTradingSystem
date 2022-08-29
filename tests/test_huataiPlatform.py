# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 10:09:25 2021

@author: chenj
"""

import traceback
import sys
import os

if __name__ == "__main__":
    try:
        sys.path.append(os.getcwd())
        from SelfTradingSystem.core.huataiPlatform2 import loginN
        from SelfTradingSystem.core.trade import Trade
        from SelfTradingSystem.io.database import Database
        sql = Database( 'Resources.db')
        sysObj = Trade('本金账本.xlsx', sql, margin_buying_disabled=True)
        app = sysObj.pywinauto_app(backend="win32")
        Application = sysObj.pywinauto_app
        app, operator = loginN(Application)
        print('test login successfully')
    except (KeyboardInterrupt, SystemExit):
        exitCode = 1
        raise
    except:
        print ("Need assisstance for unexpected error:\n {}".format(sys.exc_info()))
        traceBackObj = sys.exc_info()[2]
        traceback.print_tb(traceBackObj)
        exitCode = 1
        pass