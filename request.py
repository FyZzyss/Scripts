import requests
import psycopg2
import csv
import random
import string

from argparse import ArgumentParser

requests.packages.urllib3.disable_warnings()

HOST = ''
PASS = ''
PORT = ''
USER = ''
BASE = ''

SI_QUERY = \
    """ 
      SELECT
        request_id,
        instance_details->'parked_payment_data'->'payment_source'->'details'->>'msisdn' as msisdn,
        t.status
      FROM processing.service_instance as si
      INNER JOIN processing.transaction t ON t.service_instance_id = si.id
      WHERE
        extra->>'transaction_id' = %s
    """


def initialize_cursor():
    dsn = "dbname={} user={} password={} host={} port={}".format(
        BASE, USER, PASS, HOST, PORT
    )
    conn = psycopg2.connect(dsn)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur


def get_data_by_vrb_id(cur, mts_tx_id):
    try:
        cur.execute(SI_QUERY, (mts_tx_id,))
    except Exception as e:
        raise ValueError(str(e))
    else:
        data = cur.fetchone()
        if data is None:
            print('Transaction {} not found'.format(mts_tx_id))
        else:
            return str(data[0]), str(data[1]), str(data[2])


def request_id_to_mts(x: str):
    if '-' in x:
        return x[:8] + x[9:13] + x[14:18] + x[19:23] + x[24:]
    else:
        return x


'''def request_id_to_sflabs(x: str):
    if '-' in x:
        return x
    else:
        return x[:8] + '-' + x[9:13] + '-' + x[14:18] + '-' + x[19:23] + '-' + x[24:]'''


def randomword(length=5):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))


if __name__ == '__main__':
    parser = ArgumentParser(description="MTS Ewallet")
    parser.add_argument(
        "-c",
        "--csv",
        help="Name of csv_file from MTS",
        action="store",
        dest="csv",
        required=True
    )
    parser.add_argument(
        "-nh",
        "--no_header",
        help="Means that csv doesn't contain header ",
        action='store_true',
        dest="no_header"
    )
    parser.add_argument(
        '-m',
        "--mdOrderFld",
        help="Number of column in csv for getting vrb_id",
        action="store",
        dest="vrb_id",
        type=int,
        required=True
    )
    parser.add_argument(
        '-p',
        "--phoneFld",
        help="Number of column in csv for getting msisdn",
        action="store",
        dest="msisdn",
        type=int,
        required=False
    )
    args = parser.parse_args()
    with open(args.csv, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        if args.no_header:
            pass
        else:
            next(spamreader)
        cur = initialize_cursor()
        with open(randomword() + '.csv', 'w', newline='') as new_file:
            fieldnames = ['vrd_id', 'msisdn', 'mts_status', 'mts_errorcode']
            writer = csv.DictWriter(new_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in spamreader:
                if args.msisdn:
                    data = (row[args.vrb_id], row[args.msisdn])
                else:
                    data = get_data_by_vrb_id(cur, row[args.vrb_id])
                r = requests.post(
                    f'https://ewallet.mts.ru/ewallet/3.4/random/getStatus.do?phone={data[1]}&partnerMdOrder={request_id_to_mts(data[0])}',
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}, cert=('random.crt', 'random.key'),
                    verify=False)
                writer.writerow({'vrd_id': request_id_to_mts(data[0]), 'msisdn': data[1], 'mts_status': r.json()['state'],
                                 'mts_errorcode': r.json()['errorCode']})

        print(f'New file is {new_file.name}')
