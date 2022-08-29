# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 20:50:14 2020

@author: chenj
"""
import autopy
import random
import time
time.sleep(2)
autopy.key.type_string('XXXX',wpm= random.randint(100, 200))
time.sleep(0.5)
autopy.key.tap(autopy.key.Code.TAB)
time.sleep(0.5)
autopy.key.type_string('XXXX',wpm= random.randint(150, 300))