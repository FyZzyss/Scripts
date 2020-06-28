import os
import smtplib
import paramiko

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, COMMASPACE
from os.path import basename

from incoming.logger import memory_logger

logger = memory_logger.get_logger()

host = os.getenv('RTK_SFTP_HOST')
user = os.getenv('RTK_SFTP_USER')
secret = os.getenv('RTK_SFTP_PASSWD')
port = os.getenv('RTK_SFTP_PORT')
sftp_from_path = os.getenv('RTK_SFTP_FROM_PATH')


def send_mail(server, send_from, send_to, send_bcc, subject, text, files=None):
    logger.info('E-mail is sending to the address {}...'.format(send_to + send_bcc))
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Bcc'] = COMMASPACE.join(send_bcc)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for file_to_send in files or []:
        with open(file_to_send, "rb") as file:
            part = MIMEApplication(
                file.read(),
                Name=basename(file_to_send)
            )

        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(file_to_send)
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to + send_bcc, msg.as_string())
    smtp.close()
    logger.info('E-mail was successfully sent to the address {}'.format(send_to + send_bcc))

sflabs_path = '/root/outgoing/transfers/RTK/from_mts/'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=host, username=user, password=secret, port=port)
ftp = client.open_sftp()
files = ftp.listdir(sftp_from_path)
if files:
    for file in files:
        try:
            ftp.get(sftp_from_path + file, sflabs_path + file)
            send_mail(
                '',
                '',
                [''],
                [],
                'Расхождения с MTS_Bank',
                '',
                [sflabs_path+file]
            )
            os.remove(sflabs_path+file)
            ftp.remove(sftp_from_path + file)
        except Exception as e:
            logger.error('{}'.format(str(e)))
client.close()
