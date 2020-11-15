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

def sendEmail(subject, content, receiverEmail ="j.chen-2@tudelft.nl", imagePath=None):
    senderEmail = "bensnow0@gmail.com"
    senderPass  = "aJIntL7&Fx"
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
    sendEmail('Test', 'This is a test email.', 'chenjiayi_344@hotmail.com')

