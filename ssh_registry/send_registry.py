import datetime
import os
import smtplib

import psycopg2
import csv
import paramiko

from argparse import ArgumentParser, ArgumentTypeError
from incoming.logger import memory_logger
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, COMMASPACE
from os.path import basename

HOST = os.getenv('YURT_DB_HOST')
PASS = os.getenv('YURT_DB_PASS')
PORT = os.getenv('YURT_DB_PORT')
USER = os.getenv('YURT_DB_USER')
BASE = os.getenv('YURT_DB_BASE')

host = os.getenv('RTK_SFTP_HOST')
user = os.getenv('RTK_SFTP_USER')
secret = os.getenv('RTK_SFTP_PASSWD')
port = os.getenv('RTK_SFTP_PORT')
sftp_to_path = os.getenv('RTK_SFTP_TO_PATH')

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


def validate_format_date(from_date):
    try:
        valid_from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    except ValueError:
        raise ArgumentTypeError("{} is an invalid date".format(from_date))
    else:
        return valid_from_date


parser = ArgumentParser(description="RTK Registry")
parser.add_argument(
    help="From date",
    action="store",
    dest="from_date",
    type=validate_format_date
)

args = parser.parse_args()
logger = memory_logger.get_logger()

GetRegistry = '''
SELECT t.*,o.*,ro.* FROM
sfmobile_api.order o
join sfmobile_api.tx_tcell t on t.order_id=o.id
join sfmobile_api.rtk_order ro on ro.id=o.rtkt_id
WHERE ro.check_status=1 and ro.payment_status=1 and payment_term_time >= (%(date)s)::date 
and payment_term_time < (%(date)s)::date + interval '1 day'
'''

dsn = "dbname={} user={} password={} host={} port={}".format(
    BASE, USER, PASS, HOST, PORT
)
conn = psycopg2.connect(dsn)
conn.set_session(autocommit=True)
cur = conn.cursor()
date = args.from_date.replace(tzinfo=None).strftime('%d.%m.%Y')
file = "/root/outgoing/transfers/RTK/to_mts/random_{}.csv".format(date)
cur.execute(GetRegistry, {'date': date})
transactions = cur.fetchall()
with open(file, "w", encoding='cp1251') as csv_file:
    writer = csv.writer(csv_file, delimiter=';')
    writer.writerow(["ID платежа в ПХ", "ID платежа", "Дата платежа", "Номер телефона", "Сумма зачисления"])
    for transaction in transactions:
        writer.writerow((transaction[39], transaction[24], transaction[30].replace(tzinfo=None), transaction[27], transaction[32]))
try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=secret, port=port)
    ftp = client.open_sftp()
    ftp.put(file, sftp_to_path + 'SFLabs_{}.csv'.format(date))
    send_mail(
        '',
        '',
        [''],
        [''],
        'Реестр за {}'.format(date),
        '',
        [file]
    )
except Exception as e:
    logger.error('{}'.format(str(e)))
finally:
    os.remove(file)
