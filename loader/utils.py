# -*- coding: utf-8 -*-
import smtplib
from email.mime.text import MIMEText


def send_email(subject, text, to=''):
    HOST = 'smtp.exmail.qq.com'
    USER = 'xx@tt.com'
    PASSWD = 'xxzz'
    FROM = USER
    TO = 'xx@yy.com'
    if to:
        TO = '%s;%s' %(TO, to)
    # init smtp
    smtp = smtplib.SMTP(HOST)
    # login
    smtp.login(USER, PASSWD)
    # set message body, subject, from, to
    msg = MIMEText(text, 'html', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = FROM
    msg['To'] = TO
    # send
    smtp.sendmail(FROM, TO.split(';'), msg.as_string())
    smtp.close()