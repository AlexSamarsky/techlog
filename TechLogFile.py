import os
import multiprocessing as mp
import re
from datetime import date, datetime
from typing import AnyStr, Iterable, Iterator, List, Match

from Timer import Timer
from pathlib import Path

class TechLogFile:
    __pattern_start_record: re.Pattern = re.compile(r"^\d{2,12}:\d{2}\.\d{6}-\d+,")
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

    def get_time(self) -> List[date]:
        return [datetime.strptime(self._start_time, self.__strptime), datetime.strptime(self._end_time, self.__strptime)]

    def _string_process_with_write(self, line: str) -> None:
        new_line: str = self.string_process(line, self._current_file_name)
        self._write_stream(new_line)
    
    def filter_line(self, line: str, current_file_name: str = None) -> str:
        return line
    
    def filter_time(self, event_time: str) -> int:
        if not self._filter_time:
            return 0
        len_event: int = len(event_time)
        if (self._start_time and self._start_time[:len_event] < event_time):
            return -1
        if (self._end_time and self._end_time[:len_event] > event_time):
            return 1
        return 0
    
    def filter_file(self, file_name: str) -> str:
        if not self.filter_time(file_name):
            return ''
        return file_name

    def string_process(self, line: str, current_file_name: str = None) -> str:
        new_line: str = line.strip("\n").replace('\n', '\\n')
        if self._filter_time:
            mmss: str = line[:5].replace(':', '', 1)
            time_string: str = f'{current_file_name}{mmss}'
            if not self.filter_time(time_string):
                return ''
        new_line = self.filter_line(new_line, self._current_file_name)
        return new_line

    def _write_stream(self, new_line: str) -> None:
        if not new_line:
            return
        if not new_line.endswith('\n'):
            new_line += '\n'
        self._file_out_stream.write(new_line)

    def _worker_string_process(self, input, output):
        """Функция, выполняемая рабочими процессами"""
        for line in iter(input.get, 'STOP'):
            result = self._string_process_with_write(line)
            self._write_stream(result)
            output.put(result)

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

    def __chunkify(self) -> Iterator[str]:
        files_arr: List[str] = self.__get_files()
        file_name: str = ''
        line: str = ''
        
        if files_arr:
            for file_name in files_arr:
                file_name = self.filter_file(file_name)
                if file_name:
                    # self._current_file_name = os.path.basename(file_name)
                    self._current_file_name: str = Path(file_name).stem
                    with open(file_name, 'r', encoding=self._encoding, errors='replace') as f:
                        lines: List[str] = []
                        while True:
                            while True:
                                line = f.readline()
                                if not line:
                                    break
                                group: Match[str] = self.__pattern_start_record.match(line)
                                if group and lines:
                                    break
                                else:
                                    lines.append(line)
                            if lines:
                                yield ''.join(lines)
                            if not line:
                                break
                            lines = [line]
        yield ''
        yield ''

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

        if self._multitreading:
            task_queue = mp.Queue(self._NUMBERS_QUEUE)
            done_queue = mp.Queue(self._NUMBERS_QUEUE)
            file_read_iterator = self.__chunkify()
            many_rows = True
            for _ in range(self._NUMBERS_QUEUE):
                line = next(file_read_iterator)
                    
                if not line:
                    many_rows = False
                    task_queue.put('STOP')
                    break
                task_queue.put(line)
            
            cnt = task_queue.qsize() - 1
            
            procs_arr = []
            for _ in range(self._NUMBERS_CORES):
                proc = mp.Process(target=self._worker_string_process, args=(task_queue, done_queue))
                procs_arr.append(proc)
                proc.start()
            
            if many_rows:
                while True:
                    result = done_queue.get(timeout=1)
                    # print('\t', result)
                    line = next(file_read_iterator)
                    if not line:
                        task_queue.put('STOP')
                        break
                    task_queue.put(line)

            while cnt:
                result = done_queue.get(timeout=1)
                # print('\t', result)
                cnt -= 1
            
            task_queue.put('STOP')
            # done_queue.get(timeout=1)

            for proc in procs_arr:
                proc.join()
                proc.close()
        else:
            file_read_iterator: Iterator[str] = self.__chunkify()
            while True:
                line: str = next(file_read_iterator)
               
                self._string_process_with_write(line)
                if not line:
                    break
        
        if self._file_out_stream:
            self._file_out_stream.close()
        
        self._main_timer_file.stop()
        print(self._main_timer_file)


if __name__ == '__main__':
    file_in_name = 'logs/'
    file_in_name = 'logs_full/21081014.log'
    file_out_name = 'logs_test/test_out.log'
    tl = TechLogFile(file_in_name, file_out_name)
    tl.set_time(datetime(2021, 8, 10, 14, 0), datetime(2021, 8, 10, 14, 10))
    tl.main_process()
    