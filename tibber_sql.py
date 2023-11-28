import datetime
import mysql.connector
from writefile import TibberWriteFile

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
            TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_GET_DATA]: {e}')
        except Exception as e:
            TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_GET]: {e}')

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
            TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL]: {e}')
        except Exception as e:
            TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][MYSQL_GENERIC]: {e}')
