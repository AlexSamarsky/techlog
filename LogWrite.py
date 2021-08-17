
import os
from typing import List
from LogBase import LogBase
from LogDataclasses import TechLogEvent, TechLogPeriod, TechLogFile, RawLogProps, TimePatterns, RePatterns

class LogWriteToConsole(LogBase):

    def add_data(self, process_path: str, log_event: TechLogEvent):

        return f',process_path={process_path},filePath={log_event.event.file_path},filePos={log_event.event.file_pos}'

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
    
    def execute_begin(self) -> None:
        self._files_list: List[TechLogFile] = []
        self._current_io: TechLogFile = None
        
    def execute_end(self):
        if self._files_list:
            f: TechLogFile = None
            for f in self._files_list:
                if f.file_io:
                    f.file_io.close()
    
    def main_process(self, process_path: str, log_event: TechLogEvent):
        stem = log_event.event.time.strftime(TimePatterns.format_time_minute)
        if self._current_io:
            if self._current_io.stem == stem and self._current_io.rel_path == log_event.event.file.rel_path:
                search_cache = self._current_io
        if not search_cache:
            search_cache = list(filter(lambda x: x.stem == stem and x.rel_path == log_event.event.file.rel_path, self._files_list))
        if search_cache:
            file_io = search_cache.file_io
        else:
            file_name = f'{stem}.log'
            file_path = os.path.join(self._path_file_name, log_event.event.rel_path, file_name)
            file_io = open(self._path_file_name, 'w', encoding=self._encoding)
            file_object = TechLogFile(
                file_name = file_path,
                rel_path = log_event.event.rel_path,
                stem = stem,
                file_io = file_io,
            )
            self._files_list.append(file_object)
        file_io.write(log_event.text)