#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 11:28:44 2022

@author: frank
"""


# import scrapy

# class JisiluerbotSpider(scrapy.Spider):
#     name='jisilubot'
#     allowed_domains=['www.jisilu.cn/web/data/cb/list/']
#     start_urls=['https://www.jisilu.cn/web/data/cb/list//']

#     def parse(self, response):
#         tables=response.css()

import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# from unicodedata import normalize


# from SelfTradingSystem.util.stock import getHTML2

# URL = "https://www.jisilu.cn/web/data/cb/list"
# df_in_list = pd.read_html(URL, attrs = {'class': 'jsl-table.sticky-header'})

df = pd.read_excel('https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/static/html/csindex/public/uploads/file/autofile/closeweight/931411closeweight.xls')
# tempUrl='https://www.jisilu.cn/web/data/cb/index'
# tempHTML = getHTML2()
# tempContent = tempHTML.read()
# tempStr = tempContent.decode("utf-8")

# import urllib

# user_agent = 'Mozilla/5.0'
# headers={'Accept': 'application/json, text/plain, */*',
# 'Accept-Encoding': 'gzip, deflate, br',
# 'Accept-Language': 'en-US,en;q=0.9',
# 'Connection': 'keep-alive',

# 'Host': 'www.jisilu.cn',
# 'Referer': 'https://www.jisilu.cn/web/data/cb/index',
# 'Sec-Fetch-Dest': 'empty',
# 'Sec-Fetch-Mode': 'cors',
# 'Sec-Fetch-Site': 'same-origin',
# 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36',
# 'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
# 'sec-ch-ua-mobile': '?0',
# 'sec-ch-ua-platform': "Linux"
# } 
# headers = {'User-Agent': user_agent}
# request=urllib.request.Request(tempUrl,None,headers) 
# tempHTML = urllib.request.urlopen(request, timeout=20)




