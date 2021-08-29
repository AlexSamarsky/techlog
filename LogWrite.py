from io import TextIOWrapper
from functools import lru_cache
import shutil
import os
import re
from typing import List
from LogBase import LogBase
from LogDataclasses import TechLogEvent, TechLogPeriod, TechLogFile, RawLogProps, TimePatterns, RePatterns
from pathlib import Path
from Timer import MicroTimer
import multiprocessing as mp
import time

class LogWriteToConsole(LogBase):

    def __init__(self, name: str):
        super().__init__(name)
        self._add_data = False

    @property
    def add_data(self):
        return self._add_data
    
    @add_data.setter
    def add_data(self, data):
        self._add_data = data
    
    def generate_data(self, process_path: str, log_event: TechLogEvent):
        if self._add_data:
            return f',tla:processPath={process_path},tla:fullPath={log_event.event.file.full_path},tla:filePos={log_event.event.file_pos}'
        return ''

    def main_process(self, process_path: str, log_event: TechLogEvent):
        if log_event.text[-1] == '\n':
            print(log_event.event.time.strftime(TimePatterns.format_date_hour)+log_event.text.strip('\n')+self.generate_data(process_path,log_event))
        else:
            print(log_event.event.time.strftime(TimePatterns.format_date_hour)+log_event.text+self.generate_data(process_path,log_event))


class LogWriteToFile(LogWriteToConsole):

    def __init__(self, name: str, file_name: str) -> None:
        super().__init__(name)
        self._path_file_name: str = file_name
        self._append_to_file: str = 'w'
    
    @property
    def append_to_file(self):
        return True if self._append_to_file == 'a' else 'w'
        # return self._append_to_file

    @append_to_file.setter
    def append_to_file(self, append_to_file):
        if append_to_file:
            self._append_to_file = 'a'
        else:
            self._append_to_file = 'w'

    def execute_begin(self) -> None:
        super().execute_begin()
        self._files_list: List[TechLogFile] = []
        self._lock = mp.Lock()
        # self._writer = open(self._path_file_name,  self._append_to_file, encoding=self._encoding)

    def execute_end(self) -> None:
        super().execute_end()
        if self._files_list:
            f: TechLogFile = None
            for f in self._files_list:
                if f.file_io:
                    f.file_io.close()

    @lru_cache(maxsize=None)
    def get_file_io(self, full_path: str) -> TextIOWrapper:
        p = Path(full_path)
        if not p.parent.exists():
            try:
                p.parent.mkdir(parents=True, exist_ok=True)
            except:
                time.sleep(0.1)
                p.parent.mkdir(parents=True, exist_ok=True)
        file_io = open(full_path, self._append_to_file, encoding=self._encoding)
        search_cache = TechLogFile(
            full_path = full_path,
            file_io = file_io,
        )
        self._files_list.append(search_cache)
        return file_io

    def get_file_name_from_log_event(self, log_event: TechLogEvent) -> None:
        full_path = self._path_file_name
        return full_path

    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        # self._lock.acquire()
        full_path = self.get_file_name_from_log_event(log_event)
        file_io = self.get_file_io(full_path)
        # self._lock.release()
        # file_io.write(log_event.text.rstrip('\n').rstrip('\r')+self.generate_data(process_path,log_event)+'\n')
        file_io.write(log_event.text)

    def init_stream(self):
        if not self._path_file_name:
            return
        p = Path(self._path_file_name)
        if not p.exists() and not p.suffix:
            p.mkdir(exist_ok=True)

        for files in os.listdir(self._path_file_name):
            path = os.path.join(self._path_file_name, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)

# class LogWriteToCatalogByMinute(LogWriteToFile):
    
#     def __init__(self, name: str, file_name: str) -> None:
#         super().__init__(name, file_name)
#         p = Path(file_name)
#         if p.suffix:
#             raise ValueError ('Необходимо указать каталог')
#         if not p.exists():
#             p.mkdir(parents=True)
#         self._path_file_name = file_name
#         self._append_to_file: str = 'w'

#     def get_file_name_from_log_event(self, log_event: TechLogEvent) -> None:
#         pass

#     def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
#         stem = log_event.event.time_str[:10]
#         search_cache = None
#         if self._current_io:
#             if self._current_io.stem == stem and self._current_io.rel_path == log_event.event.file.rel_path:
#                 search_cache = self._current_io
#         if not search_cache:
#             search_cache_list = list(filter(lambda x: x.stem == stem and x.rel_path == log_event.event.file.rel_path, self._files_list))
#             if search_cache_list:
#                 search_cache = search_cache_list[0]
#         if search_cache:
#             file_io = search_cache.file_io
#         else:
#             file_name = f'{stem}.log'
#             full_path = os.path.join(self._path_file_name, log_event.event.file.rel_path, file_name)
#             full_path = full_path.replace('/', os.sep)
#             p = Path(full_path)
#             if not p.parent.exists():
#                 p.parent.mkdir()
#             file_io = open(full_path,  self._append_to_file, encoding=self._encoding)
#             search_cache = TechLogFile(
#                 full_path = full_path,
#                 rel_path = log_event.event.file.rel_path,
#                 stem = stem,
#                 file_io = file_io,
#             )
#             self._files_list.append(search_cache)
#         self._current_io = search_cache
#         file_io.write(log_event.text.strip('\n')+self.generate_data(process_path,log_event)+'\n')


class LogWriteToCatalogByField(LogWriteToFile):
    pass

    def __init__(self, name: str, file_name: str) -> None:
        super().__init__(name, file_name)
        self._pattern_field = None
        self._append_to_file: str = 'w'
        self._by_minute = False
        self._field_name = None
        self._file_name = file_name
        self._time_str_len = 8
        self._field_as_file = False

    @property
    def field_as_file(self) -> bool:
        return self._field_as_file

    @field_as_file.setter
    def field_as_file(self, field_as_file: bool) -> None:
        self._field_as_file = field_as_file

    @property
    def file_name(self) -> str:
        return self._path_file_name
    
    @file_name.setter
    def file_name(self, file_name: str) -> None:
        self._path_file_name: str = file_name
        p = Path(file_name)
        if p.suffix:
            self._is_file = True
        else:
            self._is_file = False


    @property
    def field_name(self) -> str:
        return self._field_name

    @field_name.setter
    def field_name(self, name: str) -> None:
        self._field_name = name
        if name:
            self._pattern_field = re.compile(r'\b{field_name}=(\d+)'.format(field_name=name))
        else:
            self._pattern_field = None

    @property
    def by_minute(self) -> bool:
        return self._by_minute

    @by_minute.setter
    def by_minute(self, by_minute: bool) -> None:
        self._by_minute = by_minute
        if by_minute:
            self._time_str_len = 10
        else:
            self._time_str_len = 8

    @lru_cache(maxsize=None)
    def get_file_name(self, file_name: str, rel_path: str = '', field_value: str = '') -> str:
        file_name = file_name

        file_name = f'{file_name}.log'
        full_path = self._path_file_name.replace('/', os.path.sep)
        if field_value:
            full_path = full_path.rstrip(os.path.sep) + os.path.sep + field_value

        if rel_path:
            full_path = full_path.rstrip(os.path.sep) + os.path.sep + rel_path

        if file_name:
            full_path = full_path.rstrip(os.path.sep) + os.path.sep + file_name

        return full_path

    def get_file_name_from_log_event(self, log_event: TechLogEvent) -> None:
        if self._is_file:
            return self._path_file_name
        else:
            if self._pattern_field:
                field_search = self._pattern_field.search(log_event.text)
                if not field_search:
                    field_value = 'etc'
                else:
                    field_value = field_search.group(1)
            else:
                field_value = None
            
            if self._field_as_file:
                field_value += '.log'
                full_path = self.get_file_name(field_value)
            else:
                full_path = self.get_file_name(log_event.event.time_str[:self._time_str_len], log_event.event.file.rel_path, field_value)
        return full_path

    # def execute_end(self):
    #     super().execute_end()
        # print(self.get_file_name.cache_info())

