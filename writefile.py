import os
import datetime

class TibberWriteFile:
    def write_file(self, filename, data, delete=False):
        try:
            if not os.path.exists(filename):
                with open(filename, 'a', encoding='utf-8') as f:
                    f.write(f'[{datetime.datetime.now()}][MAKE_FILE]: New file created.')
            if delete and os.path.isfile(filename):
                os.remove(filename)
                return
            if filename == 'log.txt':
                print(data)
                with open(filename, 'r', encoding='utf-8') as log:
                    lines = log.readlines()
                if len(lines) > 100:
                    with open(filename, 'w', encoding='utf-8') as log:
                        for number, line in enumerate(lines):
                            if number not in range(0, 10, 1):
                                log.write(line)
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(f'\n{data}')
        except Exception as e:
            print(f'[{datetime.datetime.now()}][FILE_ERR]: {e}')
