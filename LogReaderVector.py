
import os
from pathlib import Path
from typing import Any, Generator, List, Match
import re
from datetime import datetime, timedelta
from math import floor
from colorama import Fore, Style
import json
import numpy as np

from LogBase import LogBase
from LogDataclasses import EventsProcessObject, TechLogEvent, TechLogFile, RawLogProps, TimePatterns, RePatterns

class LogReaderBaseVector(LogBase):
    
    def __init__(self, name: str, files_path: str) -> None:
        super().__init__(name)
        self._files_path: str = files_path
        self._files_array: List[TechLogFile] = []
        self._raw_data: bool = True
        self._file_process_alg = 'numpy'
        self._cnt_on = False

    @property
    def raw_data(self) -> bool:
        return self._raw_data
    
    @raw_data.setter
    def raw_data(self, raw_data) -> None:
        self._raw_data = raw_data

    def seek_files(self) -> List[TechLogFile]:
        path_or_file: str = self._files_path
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

    def __filter_file(self, file_name) -> bool:
        if self._raw_data:
            if self.filter_time(Path(file_name).stem) != 0:
                return False

        return True
    
    def main_process(self, process_path: str) -> None:
        self.seek_files()
        if self._files_array:
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
        with open(file_name, 'r', encoding=self._encoding, errors='replace') as f:
            p: Path = Path(file_name)
            file_size: int = p.stat().st_size
            last_actual_seek_position: int = 0
            portion: int = file_size
            if not start_seek:
                start_seek: int = last_actual_seek_position
                
            for _ in range(10):
                if portion < 10_000:
                    break
                else:
                    portion = floor(portion / 10)
                    start_seek: int = last_actual_seek_position
            
                count_seeks: int = floor(file_size / portion)
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
                            match_new_line: Match[str] = RePatterns.re_new_event.match(line)
                            if match_new_line:
                                groups = match_new_line.groups()
                                event_time = time_in_file + groups[0] + groups[1]

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

    def event_process(self, process_path: str, file_object: TechLogFile, len_arr: int, index: int, event_array) -> None:
        if file_object.skip_file:
            return
        if index == 0 or index >= len_arr - 1:
            return
        
        # print (event_array)
        # return
        time_delta = timedelta(
                        minutes=int(event_array[1]),
                        seconds=int(event_array[2]),
                        microseconds=int(event_array[3])
                    )
        # event_string = event_array[0]
        event_time_str = f"{file_object.date_hour_str}{''.join(event_array[1:4])}"
        event_time = file_object.date_hour + time_delta
        if self._tech_log_period.filter_time:
            filter_time_result = self.filter_time(event_time)
            if filter_time_result == -1:
                event_line = ''
            elif filter_time_result == 0: 
                event_line = event_array[0]
            elif filter_time_result == 1:
                file_object.skip_file = True
                event_line = ''
        else:
            event_line = event_array[0]
        if not event_line:
            return
        # event_line = event_array[0]
        event_len = len(bytes(event_line, 'utf8'))
        raw_log_event = RawLogProps(
                            time=event_time,
                            file=file_object,
                            file_pos=file_object.raw_position,
                            duration=int(event_array[4]),
                            name=event_array[5],
                            level=event_array[6],
                            time_str=event_time_str
                            )
        tech_log_event = TechLogEvent(
                            text=event_line,
                            event=raw_log_event,
                            event_len=event_len,
                            )
        file_object.raw_position = tech_log_event.event.file_pos + tech_log_event.event_len
        self.execute_handlers(process_path, tech_log_event)
        # print(TechLogEvent)
    
    def event_process2(self, event_process_object: EventsProcessObject, file_object, index) -> None:
        if event_process_object.skip_group:
            return
        next_match = next(event_process_object.event_iter, None)
        if next_match is None:
            event_process_object.skip_group = True
            return
        event_array = next_match.groups()


        time_delta = timedelta(
                        minutes=int(event_array[1]),
                        seconds=int(event_array[2]),
                        microseconds=int(event_array[3])
                    )
        # event_string = event_array[0]
        event_time_str = f"{file_object.date_hour_str}{''.join(event_array[1:4])}"
        event_time = file_object.date_hour + time_delta
        if self._tech_log_period.filter_time:
            filter_time_result = self.filter_time(event_time)
            if filter_time_result == -1:
                event_line = ''
            elif filter_time_result == 0: 
                event_line = event_array[0]
            elif filter_time_result == 1:
                file_object.skip_file = True
                event_line = ''
        else:
            event_line = event_array[0]
        if not event_line:
            return
        # event_line = event_array[0]
        event_len = len(bytes(event_line, 'utf8'))
        raw_log_event = RawLogProps(
                            time=event_time,
                            file=file_object,
                            file_pos=file_object.raw_position,
                            duration=int(event_array[4]),
                            name=event_array[5],
                            level=event_array[6],
                            time_str=event_time_str
                            )
        tech_log_event = TechLogEvent(
                            text=event_line,
                            event=raw_log_event,
                            event_len=event_len,
                            )
        file_object.raw_position = tech_log_event.event.file_pos + tech_log_event.event_len
        # self.execute_handlers(process_path, tech_log_event)

        if index == -1:
            event_process_object.event_previous = tech_log_event
        
        if not event_process_object.event_previous:
            event_process_object.event_previous = tech_log_event
            return
        

        event_process_object.current_pos = next_match.start()
        event_process_object.event_count += 1
        
        # print(event_process_object.event_previous)
        
        event_process_object.event_previous = tech_log_event
        pass
    
    def process_file(self, process_path: str, file_object: TechLogFile) -> None:
        p = Path(file_object.full_path)
        file_path: Path = p.absolute()
        time_in_file = file_path.stem
        date_hour = time_in_file
        event_process_object = EventsProcessObject(process_path=process_path)
        
        with open(file_object.full_path, 'r', encoding=self._encoding, errors='replace') as f:
            
            if self._tech_log_period.filter_time and not self.__filter_file(time_in_file):
                return
            
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
            text = ''
            file_object.raw_position = f.tell()
            cnt_reads = floor(p.stat().st_size / size_read) + 2
            vec_event_process = np.vectorize(self.event_process)
            # vec_event_process2 = np.vectorize(self.event_process2)
            
            arr100 = np.r_[0:10000:1]
            
            for _ in range(cnt_reads):
                read_text = f.read(size_read)
                if read_text:
                    text += read_text
                # else:
                #     pass
                #     # read_text = RePatterns.re_new_event_sub.sub('<tlaline>\g<0>', read_text)
                #     # arr = np.char.split(read_text, sep='<tlaline>').reshape(1)[0]
                # events_iter = RePatterns.re_new_event_findall.finditer(text)
                # # eventc_count = RePatterns.re_new_event_findall.fullmatch
                # # arr = list(events_iter)
                # event_process_object.event_iter = events_iter
                # event_process_object.len_arr = len(arr100)
                # event_process_object.text = text
                # event_process_object.event_count = 0
                # event_process_object.skip_group = False
                
                # vec_event_process2(event_process_object, file_object, arr100)
                
                # if not event_process_object.skip_group:
                #     raise ValueError('not all processed')
                
                # if event_process_object.event_count == 1:
                #     arr100 = np.array([-1], dtype=np.int)
                text = text[event_process_object.current_pos:]
                
                arr = RePatterns.re_new_event_findall.findall(text)
                arr.insert(0, '')
                if not read_text:
                    arr.append('')
                np_arr = np.array(arr, dtype="object")
                indexes = np.r_[0:len(np_arr):1]
                vec_event_process(process_path, file_object, len(np_arr), indexes, np_arr)
                # break
                # break
                if not read_text:
                    break          
                else:
                    text = arr[-1][0]
            pass

