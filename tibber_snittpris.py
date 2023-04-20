import math
import mysql.connector
import datetime
import os
import mqtt_client
import asyncio
from mysecrets import SnittprisScrts


class Calculation:
    def __init__(self, data, nowindex):
        self.sqldata = data
        self.predicted = []
        self._nowindex = nowindex
        self._time_future = datetime.datetime.now().hour + 4
        self._default = [["gjennomsnitt", 1], ["low", 1], ["high", 1], ["price", 1]]
        self.avgstd = {"now": self._default, "future": self._default}
        self._calculate()

    def _calculate(self):
        self.avgstd["now"] = self._averagestdvarfunc()

    @staticmethod
    def _nettleiecalc(timenow, weekday):
        isday = 6 <= timenow < 22
        if 0 <= weekday <= 4:
            nettleie = (0.4914 if isday else 0.4114)
            return nettleie
        return 0.4114

    def _averagestdvarfunc(self):
        try:
            currentprice = round(self.sqldata[self._nowindex][1], 2)
            sums = 0
            count = len(self.sqldata)
            stdev = 0
            for price in self.sqldata:
                sums += price[1]

            avg = sums / count
            for index in range(count):
                stdev += (self.sqldata[index][1] - avg) ** 2
            stdev = math.sqrt(stdev / count)
            zscore = round((currentprice - avg)/stdev, 2)
            ret = [["price", currentprice], ["low", round(avg - (stdev / 2), 2)],
                   ["high", round(avg + (stdev / 2), 2)], ["gjennomsnitt", round(avg, 2)], ["zscore", zscore]]
            return ret
        except Exception as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][CALC]: ' + str(e))
            return self._default


class SqlClass:
    def __init__(self, host, user, passwd, database, authplugin, hrsstart):
        self._mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=passwd,
            database=database,
            auth_plugin=authplugin
        )
        self.data = []
        self._hrsstart = hrsstart

    def getdata(self, hrsstart, hrsend):
        self.data = []
        try:
            start = (datetime.datetime.now() - datetime.timedelta(hours=hrsstart)).replace(second=0, microsecond=0, minute=0)
            end = (datetime.datetime.now() + datetime.timedelta(hours=hrsend)).replace(second=0, microsecond=0, minute=0)
            mycursor = self._mydb.cursor()
            sql = "select time, total from TibberPrices where time between '" + str(start) + "' and '" + str(end) + "';"
            mycursor.execute(sql)
            tmp = mycursor.fetchall()
            for row in tmp:
                nettleie = self._nettleiecalc(int(str.split(str(row[0]), ' ')[1].split(':')[0]),
                                              datetime.datetime.strptime(str.split(str(row[0]), ' ')[0], '%Y-%m-%d').weekday())
                self.data.append((str(row[0]), float(row[1]) + nettleie))
        except mysql.connector.errors.IntegrityError as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MYSQL_GETDATA]: ' + str(e.msg))
        except Exception as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MYSQL_GET]: ' + str(e))

    @staticmethod
    def _nettleiecalc(timenow, weekday):
        isday = 6 <= timenow < 22
        if 0 <= weekday <= 4:
            nettleie = (0.486 if isday else 0.406)
            return nettleie
        return 0.406

    def storetosql(self, _allprices, _avg):
        try:
            mycursor = self._mydb.cursor()
            sql = "update TibberPrices " \
                  "SET " \
                  "price = " + str(_avg[0][1]) + ", " \
                  "stdvar_low = " + str(_avg[1][1]) + ", " \
                  "stdvar_high = " + str(_avg[2][1]) + ", " \
                  "gjsnitt = " + str(_avg[3][1]) + ", " \
                  "zscore = " + str(_avg[4][1]) + " " \
                  "where time = '" + _allprices[self._hrsstart][0] + "'"
            mycursor.execute(sql)
            self._mydb.commit()
        except mysql.connector.errors.IntegrityError as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MYSQL]: ' + str(e.msg))
        except Exception as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MYSQL_GENERIC]: ' + str(e))


class MqttClass:
    def __init__(self, base):
        self._mqttclient = mqtt_client.connect_mqtt()
        self._basetopic = base

    async def sendtomqtt(self, data):
        try:
            for i in range(len(data)):
                mqtt_client.publish(self._mqttclient, ''.join([self._basetopic, data[i][0]]), data[i][1])
                await asyncio.sleep(0.02)
        except Exception as e:
            write_file('log.txt', '[' + str(datetime.datetime.now()) + '][MQTT]: ' + str(e))


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
    try:
        start = 14
        end = 9
        scrts = SnittprisScrts()
        sqlclass = SqlClass(scrts.getip(), scrts.getusr(), scrts.getpwd(), scrts.getdb(), 'mysql_native_password', start)
        # for index in range(170):
        sqlclass.getdata(start, end)            # (start + index, end - index)
        processed_data = Calculation(sqlclass.data, start)
        asyncio.run(MqttClass("elec/python/mqtt/").sendtomqtt(processed_data.avgstd["now"]))
        sqlclass.storetosql(sqlclass.data, processed_data.avgstd["now"])
    except KeyboardInterrupt as ki:
        write_file('log.txt', '[' + str(datetime.datetime.now()) + '][INTERRUPT]: ' + str(ki))
    except Exception as e:
        write_file('log.txt', '[' + str(datetime.datetime.now()) + '][GENERIC]: ' + str(e))


if __name__ == "__main__":
    main()
