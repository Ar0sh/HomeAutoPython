import datetime
from mqtt_client import MqttClient
from writefile import TibberWriteFile


class MqttClass:
    mqtt_client = MqttClient()
    def __init__(self, base):
        self.mqttclient = self.mqtt_client.connect_mqtt()
        self.basetopic = base

    def sendtomqtt(self, data, tolower=False):
        try:
            self.mqtt_client.publishmany(self.mqttclient, self.basetopic, data, tolower)
        except Exception as e:
            TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][MQTT]: {e}')