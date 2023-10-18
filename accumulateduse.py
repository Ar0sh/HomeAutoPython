import mysql.connector
import datetime
import mqtt_client
from mysecrets import SnittprisScrts

scrt = SnittprisScrts()


def main():
    try:
        mqttclient = mqtt_client.connect_mqtt()
        mydb = mysql.connector.connect(
            host=scrt.getip(),
            user=scrt.getusr(),
            password=scrt.getpwd(),
            database=scrt.getdb(),
            auth_plugin='mysql_native_password'
        )
        datenow = datetime.datetime.now().date()
        minutenow = datetime.datetime.now().minute
        hournow = (datetime.datetime.now().hour,
                   (datetime.datetime.now().hour - 1, 23)[datetime.datetime.now().hour == 0])[minutenow == 0]
        mycursor = mydb.cursor()
        sql = "SELECT * FROM OpenHAB2.tibbermqtt_dailyaccumulatedconsumption_0361 " \
              "where time >= '" + str(datenow) + " " + str(hournow) + ":01:00' " \
              "order by time desc;"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        # if hournow != 0:
        try:
            numbertosend = result[0][1] - result[-1][1]
        except IndexError:
            numbertosend = 0
        # else:
        #    numbertosend = result[0][1]
        mqtt_client.publish(mqttclient, "elec/python/mqtt/accumulatedhourlyconsumption", numbertosend)
    except mysql.connector.errors.IntegrityError as e:
        print(e.msg)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
