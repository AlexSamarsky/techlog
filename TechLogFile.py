import os
import re
from datetime import date, datetime
from typing import List, Match, Pattern
from math import floor

from Timer import Timer
from pathlib import Path

profile = True

class TechLogFile:
    __pattern_start_record: re.Pattern = re.compile(r"^(\d{2,12}):(\d{2})\.\d{6}-\d+,")
    __strptime: str = "%y%m%d%H%M%S"

    _file_in_name = ''
    _file_out_name = ''
    _NUMBERS_CORES = 2
    _NUMBERS_QUEUE = _NUMBERS_CORES * 250
    _multitreading = False
    _file_out_stream = None
    _encoding: str = "utf-8-sig"
    # _main_timer_file = None
    _current_file_name = ''

    def __init__(self, file_in_name: str, file_out_name: str) -> None:
        self._file_in_name: str = file_in_name
        self._file_out_name: str = file_out_name
        self._main_timer_file: Timer = Timer('TechLogFile')
        self._start_time: date = None
        self._end_time: date = None
        self._filter_time: bool = False
        self._line_filters = []
    
    def set_time(self, start_time: date, end_time: date) -> None:
        if end_time < start_time:
            raise ValueError('End time can\'t be lower than start time')
        if not start_time and end_time:
            self._start_time = None
            self._end_time = None
            self._filter_time = False
            return
        
        self._filter_time = True
        if start_time:
            self._start_time = start_time.strftime(self.__strptime)
        else:
            self._start_time = None
        if end_time:
            self._end_time = end_time.strftime(self.__strptime)
        else:
            self._end_time = None

    def set_line_filters(self, list_filter):
        self._line_filters = []
        if list_filter:
            for line_filter in list_filter:
                self._line_filters.append(re.compile(r'' + line_filter))

    def get_time(self) -> List[date]:
        return [datetime.strptime(self._start_time, self.__strptime), datetime.strptime(self._end_time, self.__strptime)]

    def _string_process_with_write(self, line: str) -> None:
        new_line: str = self.string_process(line, self._current_file_name)
        self._write_stream(new_line)
    
    def filter_line(self, line: str) -> str:
        if line and self._line_filters:
            line_filtered: bool = True
            re_line_filter: Match = None
            for re_line_filter in self._line_filters:
                re_search = re_line_filter.search(line)
                if not re_search:
                    line_filtered = False
            if not line_filtered:
                line = ''
        return line
    
    def filter_time(self, event_time: str) -> int:
        if not self._filter_time:
            return 0
        len_event: int = len(event_time)
        if (self._start_time and event_time < self._start_time[:len_event]):
            return -1
        if (self._end_time and event_time > self._end_time[:len_event]):
            return 1
        return 0
    
    def filter_file(self, file_name: str) -> str:
        if not self.filter_time(file_name):
            return ''
        return file_name

    def string_process(self, line: str, current_file_name: str = None) -> str:
        # if self._filter_time:
        #     mmss: str = line[:5].replace(':', '', 1)
        #     time_string: str = f'{current_file_name}{mmss}'
        #     if not self.filter_time(time_string):
        #         return ''
        new_line: str = line.strip("\n").replace('\n', '\\n')
        new_line = self.filter_line(new_line)
        return new_line

    def _write_stream(self, new_line: str) -> None:
        if not new_line:
            return
        if not new_line.endswith('\n'):
            new_line += '\n'
        self._file_out_stream.write(new_line.replace('\\n', '\n'))

    def __get_files(self) -> List[str]:
        path_or_file: str = self._file_in_name
        files_arr: List[str] = []
        try:
            if os.path.isdir(path_or_file):
                p = Path(path_or_file)
                for f in p.glob('**/*'):
                    if os.path.isfile(f):
                # for adress, _, files in os.walk(path_or_file, topdown=False):
                #     for file in files:
                        files_arr.append(str(f))
            elif os.path.isfile(path_or_file):
                files_arr.append(path_or_file)
            return files_arr
        except FileNotFoundError:
            return files_arr
    
    def delete_out_file(self) -> None:
        if os.path.exists(self._file_out_name):
            if os.path.isdir(self._file_out_name):
                p = Path(self._file_out_name)
                for f in p.glob('*'):
                    if os.path.isfile(f):
                        os.remove(str(f))
            elif os.path.isfile(self._file_out_name):
                os.remove(self._file_out_name)
    
    def seek_position(self, file_name, start_seek: int = None) -> int:
        with open(file_name, 'r', encoding=self._encoding, errors='replace') as f:
            p: Path = Path(file_name)
            file_size: int = p.stat().st_size
            # first_portion = floor(file_size/10)
            last_actual_seek_position: int = 0
            portion: int = file_size
            if not start_seek:
                start_seek: int = last_actual_seek_position
            for _ in range(10):
                if portion < 100_000:
                    break
                else:
                    portion = floor(portion / 10)
                    start_seek: int = last_actual_seek_position
                    # f.seek(start_seek)

            
                count_seeks: int = floor(file_size / portion)
                # if file_size > 100_000_000:
                #     count_seeks = 1000
                if count_seeks:
                    founded_position = False
                    for i in range(count_seeks - 1):
                        current_seek = start_seek + i * portion
                        f.seek(current_seek)
                        if current_seek > 0:
                            f.readline()
                        for _ in range(1000):
                            seek_position = f.tell()
                            line: str = f.readline()
                            if not line:
                                founded_position = True
                                break
                            match_new_line: Match[str] = self.__pattern_start_record.match(line)
                            if match_new_line:
                                groups = match_new_line.groups()
                                event_time = self._current_file_name + groups[0] + groups[1]

                                filter_time_result = self.filter_time(event_time)
                                if filter_time_result == -1:
                                    last_actual_seek_position = seek_position
                                else:
                                    founded_position = True
                                break
                        if founded_position:
                            break
                    if not current_seek:
                        break
        return last_actual_seek_position


    def process_file(self, file_name: str) -> None:
        event_line: str = ''
        # event_time: str = ''
        filter_time_result: int = 0
        
        file_name = self.filter_file(file_name)
        if file_name:
            skip_file: bool = False
            # self._current_file_name = os.path.basename(file_name)
            self._current_file_name: str = Path(file_name).stem
            if self.filter_time(self._current_file_name) != 0:
                return

            with open(file_name, 'r', encoding=self._encoding, errors='replace') as f:
                if self._filter_time:
                    file_position: int = self.seek_position(file_name)
                    f.seek(file_position)


                lines: List[str] = []
                while True:
                    for _ in range(1000):
                        line = f.readline()
                        if not line:
                            break
                        match_new_line: Match[str] = self.__pattern_start_record.match(line)
                        if match_new_line:
                            if self._filter_time:
                                groups = match_new_line.groups()
                                next_event_time = self._current_file_name + groups[0] + groups[1]
                            if lines:
                                break
                            else:
                                event_time = next_event_time
                        else:
                            lines.append(line)
                    if lines:
                        if self._filter_time:
                            filter_time_result = self.filter_time(event_time)
                            if filter_time_result == -1:
                                event_line = ''
                            elif filter_time_result == 0: 
                                event_line = ''.join(lines)
                            elif filter_time_result == 1:
                                skip_file = True
                                event_line = ''
                        else:
                            event_line = ''.join(lines)
                        self._string_process_with_write(event_line)
                    if not line or skip_file:
                        break
                    lines = [line]
                    event_time = next_event_time

    def init_file_out_stream(self):
        last_arg: str = os.path.basename(self._file_out_name)
        search_dot: re.Match[str] = re.search(r'\.', last_arg)
        if search_dot:
            self._file_out_stream = open(self._file_out_name, 'w', encoding=self._encoding)
        else:
            self._file_out_stream = None
    
    def main_process(self):

        self._main_timer_file.start()

        self.init_file_out_stream()

        files_arr: List[str] = self.__get_files()
        if files_arr:
            for file_name in files_arr:
                self.process_file(file_name)

        # file_read_iterator: Iterator[str] = self.__chunkify()
        # while True:
        #     line: str = next(file_read_iterator)
            
        #     self._string_process_with_write(line)
        #     if not line:
        #         break
        
        if self._file_out_stream:
            self._file_out_stream.close()
        
        self._main_timer_file.stop()
        print(self._main_timer_file)


def main():
    file_in_name = 'logs/'
    file_in_name = 'logs_full/rphost_4132/21081014.log'
    # file_in_name = 'web/rphost_4132/21081008.log'
    file_in_name = '//Onyx-1c-ppo2/Logz/PPO_Store_FULL/rphost_3636'
    file_out_name = 'logs_test/ppo_out.log'


    # file_in_name = 'logs_test/ppo2/21042610.log'
    # file_out_name = 'logs_test/ppo_out2.log'
    # tl = TechLogFile(file_in_name, file_out_name)
    # tl.set_time(datetime(2021, 4, 26, 10, 1), datetime(2021, 4, 26, 10, 2))
    # tl.set_line_filters([',VRSRESPONSE,|,VRSREQUEST,'])
    # # tl.set_time(datetime(2021, 8, 10, 8, 10), datetime(2021, 8, 10, 8, 20))
    # tl.main_process()


    file_in_name = 'Logs_full/rphost_4132'
    file_out_name = 'logs_test/21081615.log'
    tl = TechLogFile(file_in_name, file_out_name)
    tl.set_time(datetime(2021, 8, 16, 15, 00, 00, 41002), datetime(2021, 8, 16, 15, 0,  3, 681114))
    # tl.set_time(datetime(2021, 8, 16, 14, 59, 59, 41002), datetime(2021, 8, 16, 15, 0,  3, 681114))
    tl.set_line_filters([',t:connectID=49613'])
    # tl.set_time(datetime(2021, 8, 10, 8, 10), datetime(2021, 8, 10, 8, 20))
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
