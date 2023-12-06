import datetime
from tibber_sql import SqlClass
from writefile import TibberWriteFile
from mqtt_class import MqttClass
from mysecrets import SnittprisScrts
from tibber_calculator import Calculation

def run(start=14, end=9):
    try:
        secrets = SnittprisScrts()
        sql_class = SqlClass(secrets.getip(), secrets.getusr(), secrets.getpwd(), secrets.getdb(), 'mysql_native_password', start)
        sql_class.get_data(start, end)
        processed_data = Calculation(sql_class.data, start)
        MqttClass("elec/python/mqtt/").sendtomqtt(processed_data.avg_std["now"], tolower=True)
        sql_class.store_to_sql(sql_class.data, processed_data.avg_std)
    except KeyboardInterrupt as ki:
        TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][INTERRUPT]: {ki}')
    except Exception as e:
        TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][GENERIC]: {e}')

def main():
    run()

if __name__ == "__main__":
    main()
