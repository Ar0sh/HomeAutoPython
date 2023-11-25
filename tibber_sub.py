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


class MqttClass:
    def __init__(self, base):
        self.mqttclient = mqtt_client.connect_mqtt()
        self.basetopic = base

    def sendtomqtt(self, data):
        mqtt_client.publishmany(self.mqttclient, self.basetopic, data)


async def subscribe():
    async with aiohttp.ClientSession() as session:
        tibber_connection = tibber.Tibber(ACCESS_TOKEN, websession=session, user_agent="Heimen2", time_zone=datetime.timezone.utc)
        await tibber_connection.update_info()
    home = tibber_connection.get_homes()[0]
    await home.rt_subscribe(_callback)


async def main():
    tasks = [subscribe(), asyncio.sleep(10)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except KeyboardInterrupt as ex:
        print(ex)
    finally:
        loop.close()
