#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 23:37:02 2020

@author: frank
"""

import time
from PIL import Image

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


def sleep(seconds): #for KeyboardInterrupt 
    for i in range(seconds):
        time.sleep(1)