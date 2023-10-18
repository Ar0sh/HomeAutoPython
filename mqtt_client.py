import time
from paho.mqtt import client as mqtt_client
import random
from mysecrets import SnittprisScrts

scrt = SnittprisScrts()

broker = scrt.getip()
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
    if data is not None:
        client.publish(topic, data)


def publishmany(client, basetopic, data):
    for signal, values in data:
        if values is not None:
            client.publish(''.join([basetopic, signal.capitalize()]), values)
        time.sleep(2/1000)
