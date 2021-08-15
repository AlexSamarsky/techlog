import re
import os
import sys
from datetime import datetime
from TechLogFile import TechLogFile


profile = True

class TechLogByField(TechLogFile):
    
    _pattern_contains_folder = None
    _array_files = []

    def __init__(self, file_in_name: str, file_out_name: str, field_name: str):
        super().__init__(file_in_name, file_out_name)
        if os.sep == '\\':
            sep = os.sep*2
        else:
            sep = os.sep
        self._file_out_name = re.sub(r'/', sep, self._file_out_name)
        string_contains_folder = r'' + sep
        self._pattern_contains_folder = re.compile(string_contains_folder)
        self._pattern_filter_file = re.compile(r'rphost_*')
        self._array_files = []
        self._field_value = ''

        self._field_name = field_name
        self._pattern_field = re.compile(r'\b' + field_name + '=(\d+)')

    def string_process(self, line: str, current_file_name: str):
        line = super().string_process(line, current_file_name)
        if not line:
            return line
        line_search = self._pattern_field.search(line)
        if not line_search:
            return ''
        self._field_value = line_search.groups()[0]
        return self._current_file_name + line

    def _write_stream(self, new_line):
        if not new_line:
            return
        if not new_line.endswith('\n'):
            new_line += '\n'

        search_cache = list(filter(lambda x: x[0] == self._field_value, self._array_files))
        if search_cache:
            _, write_stream = search_cache[0]
        else:
            if len(self._field_value) > 50:
                raise Exception (f'Не возможно создать файл с длинным именем, проверьте имя поля\n{self._field_value}')
            file_name = os.path.join(self._file_out_name, f'{self._field_name}_{self._field_value}.log')
            write_stream = open(file_name, 'w', encoding=self._encoding, errors='replace')
            # write_mode = 'w'
            self._array_files.append([self._field_value, write_stream])
        write_stream.write(new_line)
        # with open(file_name, write_mode, encoding=self._encoding, errors='replace') as f:
        #     f.write(new_line)

        # self._file_out_stream.write(new_line)

    def filter_file(self, file_name: str) -> str:
        file_name = super().filter_file(file_name)
        if not file_name:
            return ''
        
        file_contains_folder = self._pattern_contains_folder.search(file_name)
        if file_contains_folder:
            filter_file_search = self._pattern_filter_file.search(file_name)
            if not filter_file_search:
                return ''
        
        return file_name

    def main_process(self):
        super().main_process()
        for _, write_stream in self._array_files:
            write_stream.close()

def main():
    # file_in_name = 'logs_full'
    # file_out_name = 'logs_test/req'
    # field_name = 'connectID'
    # tl = TechLogByField(file_in_name, file_out_name, field_name)
    # tl.set_time(datetime(2021, 8, 10, 13, 50), datetime(2021, 8, 10, 13, 51))
    # tl.delete_out_file()
    # tl.main_process()



    file_in_name = 'logs_full'
    file_out_name = 'logs_test/web'
    field_name = 'connectID'
    tl = TechLogByField(file_in_name, file_out_name, field_name)
    tl.set_time(datetime(2021, 8, 10, 13, 50), datetime(2021, 8, 10, 13, 51))
    tl.delete_out_file()
    tl.main_process()

if __name__ == '__main__':
    
    if profile:

        import cProfile, pstats
        profiler = cProfile.Profile()
        profiler.enable()
        
        main()

        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats('tottime')
        stats.print_stats(10)
    else:
        main()