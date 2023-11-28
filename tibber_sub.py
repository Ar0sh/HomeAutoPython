import asyncio
import datetime
import aiohttp
import tibber
import tibber.const
from mqtt_class import MqttClass
from mysecrets import SnittprisScrts


class TibberClass:
    def _callback(self, pkg):
        # print(pkg)
        data = pkg.get("data")
        if data is None:
            return
        print(data.get("liveMeasurement").items())
        MqttClass("TibberLive/").sendtomqtt(data.get("liveMeasurement").items())

    async def subscribe(self):        
        scrts = SnittprisScrts()
        accsess_token = scrts.getbearer()
        async with aiohttp.ClientSession() as session:
            tibber_connection = tibber.Tibber(accsess_token, websession=session, user_agent="Heimen2", time_zone=datetime.timezone.utc)
            await tibber_connection.update_info()
        home = tibber_connection.get_homes()[0]
        await home.rt_subscribe(self._callback)


    async def main(self):
        tasks = [self.subscribe(), asyncio.sleep(10)]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(TibberClass().main())
        loop.run_forever()
    except KeyboardInterrupt as ex:
        print(ex)
    finally:
        loop.close()
