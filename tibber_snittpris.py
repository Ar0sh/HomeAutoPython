import math
import mysql.connector
import datetime
import os
import mqtt_client
import asyncio
import statistics
from mysecrets import SnittprisScrts


class Calculation:
    def __init__(self, data, now_index):
        self.sql_data = data
        self.predicted = []
        self._now_index = now_index
        self._future_index = now_index + 4
        self._default = [["gjennomsnitt", 1], ["low", 1], ["high", 1], ["price", 1]]
        self.avg_std = {"now": self._default, "future": self._default}
        self._calculate()

    def _calculate(self):
        self.avg_std["now"] = self._average_std_var_func(self._now_index)
        self.avg_std["future"] = self._average_std_var_func(self._future_index)

    def _average_std_var_func(self, index):
        try:
            prices = [price[1] for price in self.sql_data]
            current_price = round(self.sql_data[index][1], 2)
            avg = sum(prices) / len(prices)
            stdev = statistics.stdev(prices)
            zscore = round((current_price - avg) / stdev, 2)
            result = [["price", current_price],
                      ["low", round(avg - (stdev / 2), 2)],
                      ["high", round(avg + (stdev / 2), 2)],
                      ["gjennomsnitt", round(avg, 2)], ["zscore", zscore]]
            return result
        except ZeroDivisionError as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][DIV_ZERO]: {e}')
            return self._default
        except Exception as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][CALC]: {e}')
            return self._default


class SqlClass:
    def __init__(self, host, user, passwd, database, auth_plugin, hrs_start):
        self._my_db = mysql.connector.connect(
            host=host,
            user=user,
            password=passwd,
            database=database,
            auth_plugin=auth_plugin
        )
        self.data = []
        self._hrs_start = hrs_start
        self._hrs_future = hrs_start + 4

    def get_data(self, hrs_start, hr_send):
        self.data = []
        try:
            start = (datetime.datetime.now() - datetime.timedelta(hours=hrs_start)).replace(second=0, microsecond=0, minute=0)
            end = (datetime.datetime.now() + datetime.timedelta(hours=hr_send)).replace(second=0, microsecond=0, minute=0)
            with self._my_db.cursor() as my_cursor:
                sql = "SELECT time, total FROM TibberPrices WHERE time BETWEEN %s AND %s;"
                my_cursor.execute(sql, (start, end))
                tmp = my_cursor.fetchall()
                for row in tmp:
                    nettleie = self.calculate_net_fee(int(str.split(str(row[0]), ' ')[1].split(':')[0]),
                                                  datetime.datetime.strptime(str.split(str(row[0]), ' ')[0], '%Y-%m-%d').weekday())
                    self.data.append((str(row[0]), float(row[1]) + nettleie))
        except mysql.connector.errors.IntegrityError as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_GET_DATA]: {e}')
        except Exception as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_GET]: {e}')

    def calculate_net_fee(self, current_time, weekday):
        is_daytime = 6 <= current_time < 22
        net_fee = 0.516 if (0 <= weekday <= 4 and is_daytime) else 0.436
        return net_fee

    def store_to_sql(self, _all_prices, _avg):
        try:
            with self._my_db.cursor() as my_cursor:
                sql = ("UPDATE TibberPrices SET price = %s, stdvar_low = %s, stdvar_high = %s, "
                       "gjsnitt = %s, zscore = %s WHERE time = %s")
                val = (_avg["now"][0][1], _avg["now"][1][1], _avg["now"][2][1], _avg["now"][3][1], _avg["now"][4][1], _all_prices[self._hrs_start][0])
                my_cursor.execute(sql, val)
                self._my_db.commit()
                sql_2 = ("UPDATE TibberPrices SET price = %s, stdvar_low = %s, stdvar_high = %s, "
                       "gjsnitt = %s, zscore = %s WHERE time = %s")
                val_2 = (_avg["future"][0][1], _avg["future"][1][1], _avg["future"][2][1], _avg["future"][3][1], _avg["future"][4][1], _all_prices[self._hrs_future][0])
                my_cursor.execute(sql_2, val_2)
                self._my_db.commit()
        except mysql.connector.errors.IntegrityError as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL]: {e}')
        except Exception as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_GENERIC]: {e}')


class MqttClass:
    def __init__(self, base):
        self._mqtt_client = mqtt_client.connect_mqtt()
        self._base_topic = base

    async def send_to_mqtt(self, data):
        try:
            for item in data:
                mqtt_client.publish(self._mqtt_client, f'{self._base_topic}{item[0]}', item[1])
                await asyncio.sleep(0.02)
        except Exception as e:
            write_file('log.txt', f'[{datetime.datetime.now()}][MQTT]: {e}')


def write_file(filename, data, delete=False):
    try:
        if not os.path.exists(filename):
            with open(filename, "a") as f:
                f.write(f'[{datetime.datetime.now()}][MAKE_FILE]: New file created.')
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
        with open(filename, 'a') as f:
            f.write(f'\n{data}')
    except Exception as e:
        print(f'[{datetime.datetime.now()}][FILE_ERR]: {e}')


def main(start=14, end=9):
    try:
        secrets = SnittprisScrts()
        sql_class = SqlClass(secrets.getip(), secrets.getusr(), secrets.getpwd(), secrets.getdb(), 'mysql_native_password', start)
        sql_class.get_data(start, end)
        processed_data = Calculation(sql_class.data, start)
        asyncio.run(MqttClass("elec/python/mqtt/").send_to_mqtt(processed_data.avg_std["now"]))
        sql_class.store_to_sql(sql_class.data, processed_data.avg_std)
    except KeyboardInterrupt as ki:
        write_file('log.txt', f'[{datetime.datetime.now()}][INTERRUPT]: {ki}')
    except Exception as e:
        write_file('log.txt', f'[{datetime.datetime.now()}][GENERIC]: {e}')


if __name__ == "__main__":
    main()
