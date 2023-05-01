import tibber.const
import asyncio

import aiohttp
import tibber
import datetime
import mqtt_client
from mysecrets import SnittprisScrts

scrts = SnittprisScrts()
ACCESS_TOKEN = scrts.getbearer()


def _callback(pkg):
    # print(pkg)
    data = pkg.get("data")
    if data is None:
        return
    MqttClass("TibberLive/").sendtomqtt(data.get("liveMeasurement").items())
    #for signal, values in data.get("liveMeasurement").items():
    #    print(signal, " ", values)


class MqttClass:
    def __init__(self, base):
        self.mqttclient = mqtt_client.connect_mqtt()
        self.basetopic = base

    def sendtomqtt(self, data):
        for signal, values in data:
            mqtt_client.publish(self.mqttclient, ''.join([self.basetopic, signal.capitalize()]), values)


async def run():
    async with aiohttp.ClientSession() as session:
        tibber_connection = tibber.Tibber(ACCESS_TOKEN, websession=session, user_agent="Heimen", time_zone=datetime.timezone.utc)
        await tibber_connection.update_info()
    home = tibber_connection.get_homes()[0]
    await home.rt_subscribe(_callback)

    while True:
        await asyncio.sleep(10)


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())
    except KeyboardInterrupt as ex:
        print(ex)


if __name__ == "__main__":
    main()
