import mysql.connector
import datetime
from mqtt_client import MqttClient
from mysecrets import SnittprisScrts

secrets = SnittprisScrts()


def main():
    try:
        mqttclient = MqttClient()
        client = mqttclient.connect_mqtt()
        with mysql.connector.connect(
            host=secrets.getip(),
            user=secrets.getusr(),
            password=secrets.getpwd(),
            database=secrets.getdb(),
            auth_plugin='mysql_native_password'
        ) as my_db:
            date_now = datetime.datetime.now().date()
            _, hour_now = divmod(datetime.datetime.now().hour, 24)
            with my_db.cursor() as my_cursor:
                sql = f"SELECT * FROM OpenHAB2.tibbermqtt_dailyaccumulatedconsumption_0361 " \
                      f"WHERE time >= '{date_now} {hour_now:02d}:01:00' " \
                      f"ORDER BY time DESC;"
                my_cursor.execute(sql)
                result = my_cursor.fetchall()
                try:
                    number_to_send = result[0][1] - result[-1][1]
                except IndexError:
                    number_to_send = 0
                mqttclient.publish(client, "elec/python/mqtt/accumulatedhourlyconsumption", number_to_send)
    except mysql.connector.errors.IntegrityError as e:
        print(e.msg)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
