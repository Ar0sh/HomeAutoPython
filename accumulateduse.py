import mysql.connector
import datetime
import mqtt_client
from mysecrets import SnittprisScrts

scrt = SnittprisScrts()


def main():
    try:
        mqttclient = mqtt_client.connect_mqtt()
        with mysql.connector.connect(
            host=scrt.getip(),
            user=scrt.getusr(),
            password=scrt.getpwd(),
            database=scrt.getdb(),
            auth_plugin='mysql_native_password'
        ) as mydb:
            datenow = datetime.datetime.now().date()
            hournow, _ = divmod(datetime.datetime.now().hour - 1, 24)
            with mydb.cursor() as mycursor:
                sql = f"SELECT * FROM OpenHAB2.tibbermqtt_dailyaccumulatedconsumption_0361 " \
                      f"WHERE time >= '{datenow} {hournow:02d}:01:00' " \
                      f"ORDER BY time DESC;"
                mycursor.execute(sql)
                result = mycursor.fetchall()
                try:
                    numbertosend = result[0][1] - result[-1][1]
                except IndexError:
                    numbertosend = 0
                mqtt_client.publish(mqttclient, "elec/python/mqtt/accumulatedhourlyconsumption", numbertosend)
                print(f"Published {numbertosend} to mqtt.")
    except mysql.connector.errors.IntegrityError as e:
        print(e.msg)
    except Exception as e:
        print(e)
    else:
        print("Data published successfully.")


if __name__ == "__main__":
    main()
