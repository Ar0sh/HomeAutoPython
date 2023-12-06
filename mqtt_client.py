import random
import asyncio
import time
from paho.mqtt import client as mqtt_client
from mysecrets import SnittprisScrts

class MqttClient:
    secret = SnittprisScrts()
    broker = secret.getip()
    port = 1883
    client_id = f'python-mqtt-{random.randint(0, 1000)}'

    def connect_mqtt(self):
        def on_connect(rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        # Set Connecting Client ID
        client = mqtt_client.Client(self.client_id)
        client.on_connect = on_connect
        client.connect(self.broker, self.port)
        return client


    def publish(self, client, topic, data):
        if data is not None:
            client.publish(topic, data)


    async def publishmany(self, client, basetopic, data, tolower=False):
        for signal, values in data:
            if tolower:
                signal = signal.lower()
            else:
                signal = signal.capitalize()
            if values is not None:
                client.publish(''.join([basetopic, signal]), values)
            await asyncio.sleep(50/1000)
