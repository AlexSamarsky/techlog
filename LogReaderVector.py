
import os
from pathlib import Path
from typing import Any, Generator, List, Match
import re
from datetime import datetime, timedelta
from math import floor
from colorama import Fore, Style
import json
import numpy as np
import multiprocessing as mp

from LogBase import LogBase
from LogDataclasses import EventsProcessObject, TechLogEvent, TechLogFile, RawLogProps, TimePatterns, RePatterns

class LogReaderBaseVector(LogBase):
    
    def __init__(self, name: str, path_to_files: str) -> None:
        super().__init__(name)
        self._path_to_files: str = path_to_files
        self._files_array: List[TechLogFile] = []
        self._raw_data: bool = True
        self._cnt_on = False
        self._multithreading = False
        self._number_proc = 3

    @property
    def raw_data(self) -> bool:
        return self._raw_data
    
    @raw_data.setter
    def raw_data(self, raw_data) -> None:
        self._raw_data = raw_data

    def seek_files(self) -> List[TechLogFile]:
        if not self._path_to_files:
            return
        path_or_file: str = self._path_to_files
        self._files_array: List[TechLogFile] = []
        
        try:
            if os.path.isdir(path_or_file):
                p: Path = Path(path_or_file)
                for f in p.glob('**/*'):
                    if os.path.isfile(f):
                        path_list = f.parts[len(p.parts):-1]
                        if len(path_list):
                            rel_path = os.path.join(*path_list)
                        else:
                            rel_path = ''
                        self._files_array.append(TechLogFile(
                                                    full_path=str(f),
                                                    rel_path=rel_path,
                                                    init_path=path_or_file
                                                ))
                        # self.__filter_file(str(f))
            elif os.path.isfile(path_or_file):
                self._files_array.append(TechLogFile(full_path=path_or_file))
                # self.__filter_file(path_or_file)
        except FileNotFoundError:
            pass

    def _worker_file_process(self, input):
        """Функция, выполняемая рабочими процессами"""
        for line in iter(input.get, 'STOP'):
            self.process_file(line[0], line[1])
            # result = self._string_process_with_write(line)
            # self._write_stream(result)
            # output.put(True)

    def __filter_file(self, file_name) -> bool:
        if self._raw_data:
            if self.filter_time(Path(file_name).stem) != 0:
                return False

        return True
    
    def main_process(self, process_path: str) -> None:
        self.seek_files()
        if self._files_array:
            if self._multithreading:
                queue = mp.Queue(len(self._files_array)+self._number_proc)
                for file_object in self._files_array:
                    obj = (process_path, file_object)
                    queue.put(obj)
                
                proc_arr = []

                for _ in range(self._number_proc):
                    queue.put('STOP')
                    proc = mp.Process(target=self._worker_file_process, args=(queue, ))
                    proc_arr.append(proc)
                    proc.start()

                for proc in proc_arr:
                    proc.join()
                    proc.close()

                print('end of processes')
                    # self.process_file(process_path, file_object)
            else:
                for file_object in self._files_array:
                    self.process_file(process_path, file_object)
                # try:
                #     for tech_log_event in self.process_file(process_path, file_object):
                #         self.execute_handlers(process_path, tech_log_event)
                # except FileNotFoundError:
                #     pass
    
    def seek_position(self, file_name: str, start_seek: int = None) -> int:

        p = Path(file_name)
        # file_path: Path = p.absolute()
        time_in_file = p.stem
        with open(file_name, 'r', encoding=self._encoding, errors='replace', newline='') as f:
            p: Path = Path(file_name)
            file_size: int = p.stat().st_size
            last_actual_seek_position: int = 0
            portion: int = file_size
            date_hour_str = time_in_file[:8]
            date_hour = datetime.strptime(time_in_file[:8], TimePatterns.format_date_hour)

            read_size: int = 1_000_000

            if not start_seek:
                start_seek: int = last_actual_seek_position
            
            start_count: int = 0

            for _ in range(10):
                if portion < 100_000:
                    break
                else:
                    portion = floor(portion / 10)
                    start_seek: int = last_actual_seek_position
            
                count_seeks: int = floor(file_size / portion)
                if count_seeks:
                    # founded_position = False
                    for i in range(start_count, count_seeks - 1):
                        start_count = 1
                        current_seek = start_seek + i * portion
                        f.seek(current_seek)
                        # b_text = f.read(read_size)
                        text = f.read(read_size)
                        if not text:
                            break
                        if bytes(text[0], 'utf8') == b'\xef\xbf\xbd':
                            text = text[1:]
                            shift = 1
                        else:
                            shift = 0
                        search_event = RePatterns.re_new_event.search(text)
                        if not search_event:
                            # break
                            while not search_event:
                                if text[-1] == '\xef\xbf\xbd':
                                    f.seek(f.tell()-1)
                                    text = text[:-1]
                                read_text = f.read(read_size)
                                if not read_text:
                                    break
                                text += read_text
                                search_event = RePatterns.re_new_event.search(text)
                            if not search_event:
                                raise ValueError('event not found')
                        
                        minutes = search_event.group(1)
                        seconds = search_event.group(2)
                        microseconds = search_event.group(3)
                        if len(microseconds) == 4:
                            microseconds = microseconds + '00'
                        time_delta = timedelta(
                                        minutes=int(minutes),
                                        seconds=int(seconds),
                                        microseconds=int(microseconds)
                                    )
                        # event_time_str = f"{date_hour_str}{minutes}{seconds}{microseconds}"
                        event_time = date_hour + time_delta

                        filter_time_result = self.filter_time(event_time)
                        if filter_time_result == -1:
                            text_before = text[:search_event.start()]
                            len_text_before = self.calc_line_len(text_before)
                            
                            last_actual_seek_position = current_seek + len_text_before + shift

                            f.seek(last_actual_seek_position)
                            # b_text2 = f.read(10_000)
                            text2 = f.read(10_000)
                            match_event = RePatterns.re_new_event.match(text2)
                            if not match_event:
                                print('excp')
                                raise ValueError('error calculation start event')
                            pass

                        else:
                            break

                    if not current_seek:
                        break
                if last_actual_seek_position == 0:
                    break
        return last_actual_seek_position

    def event_process2(self, event_process_object: EventsProcessObject, file_object: TechLogFile, index: int) -> None:
        if event_process_object.skip_group:
            return
        

        if event_process_object.len_arr == 1:
            next_match = RePatterns.re_new_event_findall_last.match(event_process_object.text, pos=event_process_object.current_pos)
        else:
            next_match = RePatterns.re_new_event_findall.match(event_process_object.text, pos=event_process_object.current_pos)
        
        if not next_match:
            event_process_object.skip_group = True
            return

        # event_process_object.current_pos = len(event_process_object.text)
        # event_process_object.skip_group = True

        minutes = next_match.group(1)
        if len(minutes) == 10:
            file_object.date_hour_str = minutes[:8]
            file_object.date_hour = datetime.strptime(file_object.date_hour_str, TimePatterns.format_date_hour)
            minutes = minutes[8:10]
        seconds = next_match.group(2)
        microseconds = next_match.group(3)
        if len(microseconds) == 4:
            microseconds = microseconds + '00'
        time_delta = timedelta(
                        minutes=int(minutes),
                        seconds=int(seconds),
                        microseconds=int(microseconds)
                    )
        event_time_str = f"{file_object.date_hour_str}{minutes}{seconds}{microseconds}"
        event_time = file_object.date_hour + time_delta
        event_line = next_match.group(0)
        event_len = len(event_line)

        event_process_object.current_pos += event_len
        if self._tech_log_period.filter_time:
            filter_time_result = self.filter_time(event_time)
            if filter_time_result == -1:
                return
            elif filter_time_result == 0:
                pass
            elif filter_time_result == 1:
                file_object.skip_file = True
                event_process_object.skip_group = True
                return

        event_process_object.event_count += 1
        duration = next_match.group(4)
        if len(duration) == 4:
            duration += '00'
        event_process_object.tech_log_event.event.duration = int(duration)
        event_process_object.tech_log_event.event.name = next_match.group(5)
        event_process_object.tech_log_event.event.level = next_match.group(6)
        event_process_object.tech_log_event.event.time_str = event_time_str
        event_process_object.tech_log_event.text = event_line
        event_process_object.tech_log_event.event_len = event_len


        # match_begin = RePatterns.re_new_event.match(event_process_object.text, pos=event_process_object.current_pos)
        # if not match_begin and event_process_object.len_text - event_process_object.current_pos > 30:
        #     cnterror = 1


        # file_object.raw_position = event_process_object.tech_log_event.event.file_pos + self.calc_line_len(event_process_object.tech_log_event.text)
        self.execute_handlers(event_process_object.process_path, event_process_object.tech_log_event)

    def calc_line_len(self, text) -> int:
        return len(bytes(text, 'utf8')) # + text.count('\n')

    def process_file(self, process_path: str, file_object: TechLogFile) -> None:
        p = Path(file_object.full_path)
        file_path: Path = p.absolute()
        time_in_file = file_path.stem
        date_hour = time_in_file
        event_process_object = EventsProcessObject(process_path=process_path)
        
        with open(file_object.full_path, 'r', encoding=self._encoding, errors='replace', newline='') as f:
        # with open(file_object.full_path, 'rb') as f:
            
            if self._tech_log_period.filter_time and not self.__filter_file(time_in_file):
                return
            
            print(f'START {Style.BRIGHT}{Fore.YELLOW}{datetime.now()}{Fore.RESET} / {Fore.GREEN}{mp.current_process().name}{Fore.RESET} / {Fore.MAGENTA}{file_object.full_path}{Style.RESET_ALL}')
            if file_object.raw_position > 0:
                f.seek(file_object.raw_position)
            elif self._tech_log_period.filter_time and self._raw_data:
                file_position: int = self.seek_position(file_object.full_path)
                f.seek(file_position)

            if self._raw_data:
                catalog: str = file_path.parts[-2]
                match_rphost = RePatterns.re_rphost.match(catalog)
                if match_rphost:
                    rphost: int = match_rphost.groups()[0]
            
            file_object.skip_file = False
            file_object.date_hour_str = date_hour[:8]
            file_object.date_hour = datetime.strptime(date_hour[:8], TimePatterns.format_date_hour)
            size_read = 1_000_000
            event_process_object.text = ''
            
            file_object.raw_position = f.tell()
            cnt_reads = floor(p.stat().st_size / size_read) + 2
            vec_event_process2 = np.vectorize(self.event_process2)
            
            raw_log_event = RawLogProps(
                                time=datetime.now(),
                                file=file_object,
                                file_pos=0,
                                duration=0,
                                name='',
                                level='',
                                time_str=''
                                )
            tech_log_event = TechLogEvent(
                                text='', 
                                event=raw_log_event,
                                event_len=0,
                                )

            event_process_object.tech_log_event = tech_log_event
            event_process_object.f = f
            # event_process_object.current_pos_bytes = 0
            prev_position = 0
            arr100 = np.r_[0:1200:1]
            cnt_vectorize = 0
            for _ in range(cnt_reads):
                # prev_position = f.tell
                read_text = f.read(size_read)
                if read_text:
                    if read_text[-1] == '\xef\xbf\xbd':
                        f.seek(f.tell()-1)
                        read_text = read_text[:-1]
                    event_process_object.text += read_text
                else:
                    arr100 = np.array([-1], dtype=np.int)
                if not event_process_object.text:
                    file_object.raw_position = f.tell()
                    break
                elif len(event_process_object.text) > 30 * size_read:
                    print(f'error parse text, len string: {len(event_process_object.text)}')
                    # raise ValueError('Too big text')
                event_process_object.len_arr = len(arr100)
                event_process_object.event_count = 0
                event_process_object.skip_group = False
                event_process_object.current_pos = 0
                event_process_object.len_text = len(event_process_object.text)
                

                vec_event_process2(event_process_object, file_object, arr100)
                
                if not event_process_object.skip_group:
                    vec_event_process2(event_process_object, file_object, arr100)

                if not event_process_object.skip_group:
                    while not event_process_object.skip_group:
                        vec_event_process2(event_process_object, file_object, arr100)
                        cnt_vectorize += 1
                
                if not event_process_object.skip_group:
                    raise ValueError('not all processed')
                
                if file_object.skip_file:
                    file_object.raw_position = f.tell() - self.calc_line_len(event_process_object.text[event_process_object.current_pos:])
                    break
                
                event_process_object.text = event_process_object.text[event_process_object.current_pos:]
                
                if not read_text:
                    file_object.raw_position = f.tell() - self.calc_line_len(event_process_object.text[event_process_object.current_pos:])
                    # f.seek(file_object.raw_position)
                    # texttt = f.read(1000)
                    break          
            
            print(f'END   {Style.BRIGHT}{Fore.YELLOW}{datetime.now()}{Fore.RESET} / {Fore.GREEN}{mp.current_process().name}{Fore.RESET} / {Fore.MAGENTA}{file_object.full_path}{Style.RESET_ALL} / {cnt_vectorize}')

        self.execute_end_process_file_handlers(file_object)