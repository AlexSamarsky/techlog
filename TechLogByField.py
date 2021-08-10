import re
import os
import sys
from TechLogFile import TechLogFile

profile = True

class TechLogByField(TechLogFile):
    _session_id = ''
    _pattern_field = '' # re.compile(r',connectID=(\d+)')
    _pattern_file_filter = re.compile(r'rphost_*')
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
        self._array_files = []

        self._field_name = field_name
        self._pattern_field = re.compile(r'\b' + field_name + '=(\d+)')

    def string_process(self, line: str, current_file_name: str):
        line = super().string_process(line, current_file_name)
        line_search = self._pattern_field.search(line)
        if not line_search:
            return ''
        self._session_id = line_search.groups()[0]
        line = self._current_file_name + line
        return line

    def _write_stream(self, new_line):
        if not new_line:
            return
        if not new_line.endswith('\n'):
            new_line += '\n'

        search_cache = list(filter(lambda x: x[0] == self._session_id, self._array_files))
        if search_cache:
            _, write_stream = search_cache[0]
        else:
            file_name = os.path.join(self._file_out_name, f'{self._field_name}_{self._session_id}.log')
            write_stream = open(file_name, 'w', encoding=self._encoding, errors='replace')
            # write_mode = 'w'
            self._array_files.append([self._session_id, write_stream])
        write_stream.write(new_line)
        # with open(file_name, write_mode, encoding=self._encoding, errors='replace') as f:
        #     f.write(new_line)

        # self._file_out_stream.write(new_line)

    def file_filter(self, file_name):
        file_contains_folder = self._pattern_contains_folder.search(file_name)
        if file_contains_folder:
            file_filter_search = self._pattern_file_filter.search(file_name)
            if file_filter_search:
                return file_name
        else:
            return file_name
        return ''

def main():
    file_in_name = 'logs_full'
    # file_in_name = 'logs_test/test.log'
    file_out_name = 'logs_test/web'
    field_name = 'connectID'
    tl = TechLogByField(file_in_name, file_out_name, field_name)
    tl.set_time(210810135000, 210810135200)
    tl.set_time(210810065000, 210810065200)
    tl.delete_out_file()
    tl.main_process()

if __name__ == '__main__':

    if sys.platform.startswith('linux'):
        os.system('clear')
    elif sys.platform.startswith('win'):
        os.system('cls')    
    
    if profile:

        import cProfile, pstats
        profiler = cProfile.Profile()
        profiler.enable()
        
        main()

        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats('tottime')
        stats.print_stats(15)
    else:
        main()