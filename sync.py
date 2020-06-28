#!/usr/local/bin/python
import csv
import email
import imaplib
import io
import os
import re
import traceback
import zipfile
from datetime import datetime, timezone, timedelta
from email.header import decode_header

import psycopg2

from incoming.logger import memory_logger

HOST = os.getenv('DB_HOST')
PASS = os.getenv('DB_PASS')
PORT = os.getenv('DB_PORT')
USER = os.getenv('DB_USER')
BASE = os.getenv('DB_BASE')

dsn = "dbname=%s user=%s password=%s host=%s port=%s" % (BASE, USER, PASS, HOST, PORT)
conn = psycopg2.connect(dsn)
conn.set_session(autocommit=True)
curr = conn.cursor()


def parse_csv(filename):
    with open(filename, 'r', encoding='cp1251') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        slugs = {
            0: 'ext_subscription_id',
            1: 'msisdn',
            2: 'product_id',
            3: 'request_id',
            4: 'bank_amount',
            5: 'bank_date',
            6: 'external_id'
        }

        def to_nominal(x: str):
            return round(int(x) * 100)

        def to_norm(x: str):
            return x[:8] + '-' + x[8:12] + '-' + x[12:16] + '-' + x[16:20] + '-' + x[20:]

        types = {
            'bank_date': lambda dt: datetime.strptime(dt.split('+')[0].split('.')[0], '%Y-%m-%d %H:%M:%S').astimezone(
                timezone(timedelta(hours=3))) if len(dt) > 0 else None,
            'bank_amount': to_nominal,
            'request_id': to_norm
        }

        for row in reader:
            try:
                registry_row = {'bank_reg_id': filename, 'adapter': 'common_terminal'}
                for pos, slug in slugs.items():
                    row_value = row[pos]
                    registry_row[slug] = types[slug](row_value) if slug in types else row_value
                try:
                    curr.execute(
                        '''
                        select t.id as id,t.status as status
                        from processing.service_instance si
                        inner join processing.transaction t on t.service_instance_id = si.id
                        where si.request_id = %s
                          and t.context ->> 'ext_transaction_id' = %s
                        ''',
                        (registry_row['request_id'], registry_row['external_id'])
                    )
                    transaction = curr.fetchone()
                    if transaction[1] != 'success':
                        log.warning(
                            "Transaction with request_id: '{}', msisdn: '{}' and external_id: '{}' status is "
                            "'{}', status 'success' expected ".format(
                                registry_row['request_id'],
                                registry_row['msisdn'],
                                registry_row['external_id'],
                                transaction[1])
                        )
                    registry_row['transaction_id'] = transaction[0]
                    del registry_row['request_id']
                except TypeError:
                    log.warning(
                        "We do not have transaction with request_id: '{}', msisdn: '{}' and external_id: '{}'".format(
                            registry_row['request_id'],
                            registry_row['msisdn'],
                            registry_row['external_id'])
                    )
                    continue
                except Exception as e:
                    log.error(e)

                yield registry_row

            except Exception as e:
                log.error(e)
                log.error(traceback.format_exc())


def main():
    try:
        from incoming.libs.store import Storage

        storage = Storage('inplat')
        # подключаемся к ящику
        imap = imaplib.IMAP4_SSL(os.getenv('MAIL_SERVER'))
        imap.login(os.getenv('EMAIL_'), os.getenv('PASSW_'))
        imap.select()
        log.info('Logged in to mail server')
        # загружаем все непрочитанные письма, у которых в теме есть "реестр"
        imap.literal = u'DailyReport'.encode('utf-8')
        mail_list = imap.uid('search', 'charset', 'utf-8', 'UNSEEN', 'subject')[1][0].split()
        for uid in mail_list:
            try:
                # получаем письмо как набор байтов
                raw_email = imap.uid('fetch', uid, '(RFC822)')[1][0][1]

                # парсим в объект message
                message = email.message_from_bytes(raw_email)

                subject = decode_header(message.get('subject'))[0][0]
                log.info('Subject "{}"'.format(subject))

                # обрабатываем только определеные вложения
                for attachment in message.get_payload():
                    content_type = attachment.get_content_type()
                    filename = ''
                    is_zip = False

                    # если во вложении zip-файл, то получаем его как последовательность байт;
                    # с помощью ZipFile разархивируем
                    if 'application/zip' in content_type:
                        csv_attachment = attachment.get_payload(decode=True)
                        z = zipfile.ZipFile(io.BytesIO(csv_attachment))
                        filename = z.namelist()[0]
                        z.extractall()
                        z.close()
                        is_zip = True

                    if 'officedocument.spreadsheet' not in content_type and 'text/csv' not in content_type \
                            and not is_zip:
                        log.debug('Skipping part with content-type {}'.format(content_type))
                        continue

                    # определяем имя файла вложения
                    m = re.search(r'filename=\"?\'?(MBM[^\"\']+)\"?\'?', attachment.get('content-disposition'))
                    new_rows = 0
                    total_rows = 0
                    if m:
                        try:
                            if not filename:
                                # определяем параметры из названия файла
                                filename = m.group(1)
                                # заменяем все плохие символы в названии файла
                                filename = re.sub('[^\w\-_\. ]', '_', filename)

                            if 'application/zip' in content_type:
                                # если во вложении zip-архив, то получаем его размер в байтах (для логгера)
                                wrote = os.path.getsize(filename)
                            else:
                                # если во вложении просто файл (csv или xlsx), записываем его в файл filename;
                                # возвращается его размер
                                wrote = open(filename, 'wb').write(attachment.get_payload(decode=True))
                            log.info('Saved file {}, {} bytes written'.format(filename, wrote))

                            # по-разному обрабатываем xlsx и csv реестры
                            if 'text/csv' in content_type:
                                parse_function = parse_csv
                            elif 'application/zip' in content_type:
                                if '.csv' in filename:
                                    parse_function = parse_csv
                                else:
                                    raise ValueError('Unknown attachment type')
                            else:
                                raise ValueError('Unknown attachment type')
                            for row in parse_function(filename):
                                total_rows += 1
                                try:
                                    storage.store(row)
                                    new_rows += 1
                                except Exception as e:
                                    if 'duplicate key value' in str(e):
                                        log.warning(str(e).strip())
                                    else:
                                        raise e

                        except Exception as e:
                            raise e
                        finally:
                            os.remove(filename)
                            log.info('Rows in registry {}'.format(total_rows))
                            log.info('Inserted {} new rows'.format(new_rows))
                    else:
                        raise ValueError('Unable to detect filename: {}'.format(attachment.get('content-disposition')))

            except ValueError as e:
                log.error(e)
                # если была ошибка - помечаем письмо как непрочитанное
                imap.uid('store', uid, '-flags', '\SEEN')

        if len(mail_list) == 0:
            log.info('Empty message list')

    except Exception as e:
        log.error('IMAP error: {}'.format(str(e)))
        log.error(traceback.format_exc())


log = memory_logger.get_logger()

if __name__ == '__main__':
    main()

    memory_logger.close_memory_handler()
