#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 12:34:06 2019

@author: jiayichen
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
import os

def sendEmailBatch(subject, content, receiverEmail ="j.chen-2@tudelft.nl", imagePath=None):
    loopguard = 3
    while loopguard > 0:
        try:
            sendEmail(subject, content, receiverEmail, imagePath)
            loopguard = 0
        except:
            loopguard -= 1
            pass
        
def sendEmail(subject, content, receiverEmail, imagePath=None):
    senderEmail = os.environ['senderEmail']
    senderPass  = os.environ['senderPass']
    #def sendEmail(subject, content)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    
    #Next, log in to the server
    server.starttls()
    server.login(senderEmail, senderPass)
    
    #Send the mail
    
    msg = MIMEMultipart()
    msg['From'] = senderEmail
    msg['To'] = receiverEmail
    msg['Subject'] = subject
    
    
    body = content
    msg.attach(MIMEText(body, 'plain'))
    if imagePath != None:
        img_data = open(imagePath, 'rb').read()
        image = MIMEImage(img_data, name=os.path.basename(imagePath))
        msg.attach(image)
    text = msg.as_string()
    server.sendmail(senderEmail,receiverEmail, text)
    
    

if __name__ == "__main__":
    pass

