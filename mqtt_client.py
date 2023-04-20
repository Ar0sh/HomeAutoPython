import time
from paho.mqtt import client as mqtt_client
import random

broker = '192.168.1.161'
port = 1883
client_id = f'python-mqtt-{random.randint(0, 1000)}'


def connect_mqtt():
    def on_connect(rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, topic, data):
    client.publish(topic, data)


#test = connect_mqtt()
#publish(test)
