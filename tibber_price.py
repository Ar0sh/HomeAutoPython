import json
import requests
import mysql.connector
import datetime
import asyncio
import os
import time
from mysecrets import SnittprisScrts


class TibberAPIdata:
    def __init__(self, bearer):
        self._graph_url = "https://api.tibber.com/v1-beta/gql"
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

    async def get_api_data(self):
        try:
            response = requests.post(self._graph_url, headers=self._headers, data=self._data)
            response.raise_for_status()  # Raises a HTTPError if the response status is 4xx, 5xx
            json_data = response.json()
            data = json_data['data']['viewer']['homes'][0]['currentSubscription']['priceRating']['hourly']['entries']
            date_now = datetime.datetime.now().date()
            self.prices = [
                (
                    y['time'],
                    round(y['total'], 3),
                    round(y['tax'], 3),
                    y['level'],
                    round(y['energy'], 3),
                    round(y['difference'], 3),
                    y['__typename']
                )
                for y in data
                if str.split(y['time'], 'T')[0] == str(date_now + datetime.timedelta(days=1))
            ]
        except requests.exceptions.HTTPError as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][HTTP_ERR]: {e}')
        except Exception as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][GET_API]: {e}')


class SqlClass:
    def __init__(self, host, user, passwd, database, auth_plugin):
        self._my_db = mysql.connector.connect(
            host=host,
            user=user,
            password=passwd,
            database=database,
            auth_plugin=auth_plugin
        )

    def store_to_sql(self, _all_prices):
        try:
            with self._my_db.cursor() as my_cursor:
                for price in _all_prices:
                    try:
                        reduction = self.calculate_reduction(price[4])
                        actual = self.calculate_actual_price(price[1], reduction)
                        sql = "INSERT INTO TibberPrices (time, total, tax, level, energy, difference, reduction, actual) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                        val = (str(datetime.datetime.strptime(str(price[0]).split('+', maxsplit=1)[0], '%Y-%m-%dT%H:%M:%S.%f')),
                            price[1],
                            price[2],
                            price[3],
                            price[4],
                            price[5],
                            reduction,
                            actual)
                        my_cursor.execute(sql, val)
                        self._my_db.commit()
                    except Exception as e:
                        write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_DUPLICATE]: {e}')
        except mysql.connector.errors.IntegrityError as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL]: {e}')
        except Exception as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_GENERIC]: {e}')

    def calculate_reduction(self, price):
        if price > 0.70:
            return round((price - 0.70) * 0.9 * 1.25, 2)
        return 0
    
    def calculate_actual_price(self, price, reduction):
        return round(price - reduction, 3)

def write_file(filename, data, delete=False):
    try:
        if not os.path.exists(filename):
            with open(filename, "a", encoding='utf-8') as f:
                f.write(f'[{datetime.datetime.now()}][MAKE_FILE]: New file created.')
        if delete and os.path.isfile(filename):
            os.remove(filename)
            return
        if filename == 'log.txt':
            print(data)
            with open(filename, 'r', encoding='utf-8') as log:
                lines = log.readlines()
            if len(lines) > 100:
                with open(filename, 'w', encoding='utf-8') as log:
                    for number, line in enumerate(lines):
                        if number not in range(0, 10, 1):
                            log.write(line)
        with open(filename, 'a') as f:
            f.write(f'\n{data}')
    except Exception as e:
        print(f'[{datetime.datetime.now()}][FILE_ERR]: {e}')


def calc_max_min_avg(prices):
    price_values = [price[1] for price in prices]
    price_times = [datetime.datetime.strptime(str(price[0]).split('+')[0], '%Y-%m-%dT%H:%M:%S.%f') for price in prices]
    max_price = max(price_values)
    max_time = str(price_times[price_values.index(max_price)])
    min_price = min(price_values)
    min_time = str(price_times[price_values.index(min_price)])
    avg_price = round(sum(price_values) / len(price_values), 3)
    return [max_price, max_time, min_price, min_time, avg_price]

async def fetch_data(secrets):
    while True:
        tibber_api = TibberAPIdata(secrets.getbearer())
        await tibber_api.get_api_data()
        if len(tibber_api.prices) in [23, 24, 25]:
            break
        write_file('log.txt', f'[{datetime.datetime.now()}][Price]: No price found! Sleeping for 5 min!')
        await asyncio.sleep(60*5)        # Sleep for 5 min if no prices found.
    max_min_avg = calc_max_min_avg(tibber_api.prices)
    SqlClass(secrets.getip(), secrets.getusr(), secrets.getpwd(), secrets.getdb(), 'mysql_native_password').store_to_sql(tibber_api.prices)
    write_file('log.txt', f'[{datetime.datetime.now()}][Price]: Todays prices stored!')
    write_file('log.txt', f"[{datetime.datetime.now()}][MAX]: {max_min_avg[1].split(' ')[1]} {max_min_avg[0]}kr")
    write_file('log.txt', f"[{datetime.datetime.now()}][MIN]: {max_min_avg[3].split(' ')[1]} {max_min_avg[2]}kr")
    write_file('log.txt', f'[{datetime.datetime.now()}][AVG]: {max_min_avg[4]}kr')


async def main():
    scrts = SnittprisScrts()
    await fetch_data(scrts)


if __name__ == "__main__":
    asyncio.run(main())
