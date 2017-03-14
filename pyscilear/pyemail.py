import os
import sys
import smtplib
import pandas as pd

PY_VERSION = sys.version_info[0]

if PY_VERSION == 2:
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText
else:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

from logbook import debug


def ___get_email_server_connection():
    if os.path.exists('email_server'):
        ak = pd.read_csv('email_server')
        email_login = ak.values[0][0]
        email_password = ak.values[0][1]
        server_address = ak.values[0][2]
        server_port = ak.values[0][3]
        debug(ak)
    else:
        email_login = os.environ['EMAIL_LOGIN']
        email_password = os.environ['EMAIL_PASSWORD']
        server_address = os.environ['EMAIL_SERVER']
        server_port = os.environ['EMAIL_SERVER_PORT']
    return server_address, server_port, email_login, email_password


def send_email(to_address, subject='', text='', type='text', from_address=None):
    server_address, server_port, email_login, email_password = ___get_email_server_connection()
    if from_address is None:
        from_address = 'reportmailer@finquartz.com'

    if subject == '':
        subject = 'pyscilear.pyemail: test mode'
        msg = '''
                 From: Me@my.org
                Subject: testing'

                This is a test
            '''

    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject
    msg.attach(MIMEText(text, type))
    server = smtplib.SMTP(server_address, server_port)
    server.starttls()
    server.login(email_login, email_password)
    text = msg.as_string()
    server.sendmail(from_address, to_address, text)
    server.quit()


if __name__ == '__main__':
    send_email(to_address='fvernaz+pythontesting@gmail.com')
