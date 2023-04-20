import json
import requests
import mysql.connector
import datetime
import os
import time
from mysecrets import SnittprisScrts


class TibberAPIdata:
    def __init__(self, bearer):
        self._graphurl = "https://api.tibber.com/v1-beta/gql"
        self._headers = {
            'Authorization': 'Bearer ' + bearer,
            'Content-Type': 'application/json'
        }
        self._data = '{ "query": "{ viewer { homes { currentSubscription ' \
                     '{ priceRating ' \
                     '{ hourly ' \
                     '{ entries ' \
                     '{ time total tax level energy difference __typename }' \
                     '}}}}}}" }'
        self.prices = []
        self._getapidata()

    def _getapidata(self):
        try:
            tmp = requests.post(self._graphurl, headers=self._headers, data=self._data).content
            jsondata = json.loads(tmp)
            data = {'data': jsondata['data']['viewer']['homes'][0]['currentSubscription']['priceRating']['hourly']['entries']}
            datenow = datetime.datetime.now().date()
            for value in data.values():
                for y in value:
                    if str.split(y['time'], 'T')[0] == str(datenow + datetime.timedelta(days=1)):
                        self.prices.append((
                                y['time'],
                                round(y['total'], 3),
                                round(y['tax'], 3),
                                y['level'],
                                round(y['energy'], 3),
                                round(y['difference'], 3),
                                y['__typename']
                            ))
        except Exception as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][GETAPI]: ' + str(e))


class SqlClass:
    def __init__(self, host, user, passwd, database, authplugin):
        self._mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=passwd,
            database=database,
            auth_plugin=authplugin
        )

    def storetosql(self, _allprices):
        try:
            mycursor = self._mydb.cursor()
            for price in _allprices:
                try:
                    sql = "INSERT INTO TibberPrices (time, total, tax, level, energy, difference) VALUES (%s, %s, %s, %s, %s, %s)"
                    val = (str(datetime.datetime.strptime(str(price[0]).split('+')[0], '%Y-%m-%dT%H:%M:%S.%f')),
                           price[1],
                           price[2],
                           price[3],
                           price[4],
                           price[5])
                    mycursor.execute(sql, val)
                    self._mydb.commit()
                except Exception as e:
                    write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MYSQL_DUPLICATE]: ' + str(e))
        except mysql.connector.errors.IntegrityError as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MYSQL]: ' + str(e.msg))
        except Exception as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MYSQL_GENERIC]: ' + str(e))


def write_file(filename, data, delete=False):
    try:
        if delete and os.path.isfile(filename):
            os.remove(filename)
            return
        if filename == 'log.txt':
            print(data)
            with open(filename, 'r') as log:
                lines = log.readlines()
            if len(lines) > 100:
                with open(filename, 'w') as log:
                    for number, line in enumerate(lines):
                        if number not in range(0, 10, 1):
                            log.write(line)
        if os.path.isfile(filename):
            with open(filename, 'a') as f:
                f.write('\n' + data)
                f.close()
            return
        with open(filename, 'w') as f:
            f.write(data)
            f.close()
    except Exception as e:
        print('[' + str(datetime.datetime.now()) + '][FILEERR]: ' + str(e))


def main():
    scrts = SnittprisScrts()
    while True:
        data = TibberAPIdata(scrts.getbearer())
        if len(data.prices) in [23, 24, 25]:
            break
        write_file('log.txt', '[' + str(datetime.datetime.now()) + '][Price]: No price found! Sleeping for 5 min!')
        time.sleep(60*5)        # Sleep for 5 min if no prices found.
    SqlClass(scrts.getip(), scrts.getusr(), scrts.getpwd(), scrts.getdb(), 'mysql_native_password').storetosql(data.prices)


if __name__ == "__main__":
    main()
