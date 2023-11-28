import statistics
import datetime
from writefile import TibberWriteFile

class Calculation:
    def __init__(self, data, now_index):
        self.sql_data = data
        self.predicted = []
        self._now_index = now_index
        self._future_index = now_index + 4
        self._default = [["gjennomsnitt", 1], ["low", 1], ["high", 1], ["price", 1]]
        self.avg_std = {"now": self._default, "future": self._default}
        self._calculate()
        

    def _calculate(self):
        self.avg_std["now"] = self._average_std_var_func(self._now_index)
        self.avg_std["future"] = self._average_std_var_func(self._future_index)

    def _average_std_var_func(self, index):
        try:
            prices = [price[1] for price in self.sql_data]
            current_price = round(self.sql_data[index][1], 2)
            avg = sum(prices) / len(prices)
            stdev = statistics.stdev(prices)
            zscore = round((current_price - avg) / stdev, 2)
            result = [["price", current_price],
                      ["low", round(avg - (stdev / 2), 2)],
                      ["high", round(avg + (stdev / 2), 2)],
                      ["gjennomsnitt", round(avg, 2)], ["zscore", zscore]]
            return result
        except ZeroDivisionError as e:
            TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][DIV_ZERO]: {e}')
            return self._default
        except Exception as e:
            TibberWriteFile().write_file('log.txt', f'[{datetime.datetime.now()}][CALC]: {e}')
            return self._default