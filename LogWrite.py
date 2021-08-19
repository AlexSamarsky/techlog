
import os
from typing import List
from LogBase import LogBase
from LogDataclasses import TechLogEvent, TechLogPeriod, TechLogFile, RawLogProps, TimePatterns, RePatterns
from pathlib import Path

class LogWriteToConsole(LogBase):

    def add_data(self, process_path: str, log_event: TechLogEvent):

        return f',tla:processPath={process_path},tla:fullPath={log_event.event.file.full_path},tla:filePos={log_event.event.file_pos}'

    def main_process(self, process_path: str, log_event: TechLogEvent):
        if log_event.text[-1] == '\n':
            print(log_event.event.time.strftime(TimePatterns.format_date_hour)+log_event.text.strip('\n')+self.add_data(process_path,log_event))
        else:
            print(log_event.event.time.strftime(TimePatterns.format_date_hour)+log_event.text+self.add_data(process_path,log_event))


class LogWriteToFile(LogWriteToConsole):

    def __init__(self, name: str, file_name: str) -> None:
        super().__init__(name)
        self._path_file_name = file_name
        

    def execute_begin(self) -> None:
        super().execute_begin()
        self._writer = open(self._path_file_name, 'w', encoding=self._encoding)

    def execute_end(self) -> None:
        super().execute_end()
        self._writer.close()

    def main_process(self, process_path: str, log_event: TechLogEvent):
        self._writer.write(log_event.event.time.strftime(TimePatterns.format_date_hour)+log_event.text.strip('\n')+self.add_data(process_path,log_event)+'\n')

class LogWriteToCatalogByMinute(LogWriteToConsole):
    
    def __init__(self, name: str, file_name: str) -> None:
        super().__init__(name)
        p = Path(file_name)
        if p.suffix:
            raise ValueError ('Необходимо указать каталог')
        if not p.exists():
            p.mkdir(parents=True)
        self._path_file_name = file_name

    def execute_begin(self) -> None:
        self._files_list: List[TechLogFile] = []
        self._current_io: TechLogFile = None
        
    def execute_end(self):
        if self._files_list:
            f: TechLogFile = None
            for f in self._files_list:
                if f.file_io:
                    f.file_io.close()
    
    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        stem = log_event.event.time.strftime(TimePatterns.format_time_minute)
        search_cache = None
        if self._current_io:
            if self._current_io.stem == stem and self._current_io.rel_path == log_event.event.file.rel_path:
                search_cache = self._current_io
        if not search_cache:
            search_cache_list = list(filter(lambda x: x.stem == stem and x.rel_path == log_event.event.file.rel_path, self._files_list))
            if search_cache_list:
                search_cache = search_cache_list[0]
        if search_cache:
            file_io = search_cache.file_io
        else:
            file_name = f'{stem}.log'
            full_path = os.path.join(self._path_file_name, log_event.event.file.rel_path, file_name)
            full_path = full_path.replace('/', os.sep)
            p = Path(full_path)
            if not p.parent.exists():
                p.parent.mkdir()
            file_io = open(full_path, 'a', encoding=self._encoding)
            search_cache = TechLogFile(
                full_path = full_path,
                rel_path = log_event.event.file.rel_path,
                stem = stem,
                file_io = file_io,
            )
            self._files_list.append(search_cache)
        self._current_io = search_cache
        file_io.write(log_event.text.strip('\n')+self.add_data(process_path,log_event)+'\n')
