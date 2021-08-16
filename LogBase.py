import os
from pathlib import Path
from typing import List, Match
import re
from datetime import datetime
from math import floor

from LogDataclasses import TechLogEvent, TechLogPeriod, TechLogFile, TimePatterns, RePatterns

class LogBase():
    
    
    def __init__(self, name: str) -> None:
        self._handlers: List[LogBase] = []
        self._name: str = name
        self._handlers_len: int = len(self._handlers)
        self._path: str = ''
        self._event_object: TechLogEvent = None
        self._tech_log_period: TechLogPeriod = TechLogPeriod()
        self._encoding: str = "utf-8-sig"
        
    @property
    def path(self) -> str:
        return self._path
    
    @property
    def encoding(self) -> str:
        return self._encoding
    
    @property
    def get_time(self) -> TechLogPeriod:
        return self._tech_log_period
    
    def set_time(self, start_time: datetime, end_time: datetime) -> None:
        self._tech_log_period = TechLogPeriod(start_time, end_time)
        
    def filter_time(self, event_time: str) -> int:
        if not self._tech_log_period.filter_time:
            return 0
        
        if isinstance(event_time, str):
            len_event: int = len(event_time)
            if (self._tech_log_period.start_time_str and event_time < self._tech_log_period.start_time_str[:len_event]):
                return -1
            if (self._tech_log_period.end_time_str and event_time > self._tech_log_period.end_time_str[:len_event]):
                return 1
            return 0
        elif isinstance(event_time, datetime):
            if (self._tech_log_period.start_time and event_time < self._tech_log_period.start_time):
                return -1
            if (self._tech_log_period.end_time and event_time > self._tech_log_period.end_time):
                return 1
            return 0
            
    def connect(self, handler) -> None:
        self._handlers.append(handler)
        self._handlers_len = len(self._handlers)
    
    def main_process(self, event_object: TechLogEvent) -> None:
        pass
        # self._path += "/" + super().path
        # self._event_object = event_object
        # if self.process() and self._handlers_len:
        #     self.execute_handlers(event_object)
    
    def execute_handlers(self, event_object: TechLogEvent) -> None:
        for h in self._handlers:
            h.main_process(event_object)        
    
    def process(self) -> bool:
        return True

class LogReaderBase(LogBase):
    
    def __init__(self, name, files_path):
        super().__init__(name)
        self._files_path: str = files_path
        self._files_array: List[TechLogFile] = []
        self._raw_data: bool = True

    @property
    def raw_data(self) -> bool:
        return self._raw_data
    
    def __seek_files(self) -> List[str]:
        path_or_file: str = self._files_path
        self._files_path: List[TechLogFile] = []
        try:
            if os.path.isdir(path_or_file):
                p: Path = Path(path_or_file)
                for f in p.glob('**/*'):
                    if os.path.isfile(f):
                        self._files_path.append(TechLogFile(str(f)))
                        # self.__filter_file(str(f))
            elif os.path.isfile(path_or_file):
                self._files_path.append(TechLogFile(path_or_file))
                # self.__filter_file(path_or_file)
        except FileNotFoundError:
            pass

    def __filter_file(self, file_name) -> bool:
        if self._raw_data:
            if self.filter_time(Path(file_name).stem) != 0:
                return False

        return True
    
    def main_process(self) -> None:
        self.__seek_files()
        if self._files_path:
            for file_object in self._files_path:
                self.process_file(file_object)
    
    def seek_position(self, file_name) -> int:
        with open(file_name, 'r', encoding=self._encoding, errors='replace') as f:
            portion: int = 1_000_000
            p: Path = Path(file_name)
            file_size: int = p.stat().st_size
            count_seeks: int = floor(file_size / portion)
            last_actual_seek_position: int = 0
            
            if count_seeks:
                founded_position: bool = False
                for i in range(count_seeks - 1):
                    f.seek(i * portion)
                    if i > 0:
                        f.readline()
                    for _ in range(1000):
                        seek_position: int = f.tell()
                        line: str = f.readline()
                        match_new_line: Match[str] = RePatterns.re_new_event.match(line)
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
        return last_actual_seek_position

    def process_file(self, file_object: TechLogFile) -> None:
        file_path: Path = Path(file_object.file_name).absolute()
        with open(file_object.file_name, 'r', encoding=self._encoding, errors='replace') as f:
            
            if self._tech_log_period.filter_time and not self.__filter_file(file_object.file_name):
                return
            
            if file_object.raw_position > 0:
                f.seek(file_object.raw_position)
            elif self._tech_log_period.filter_time and self._raw_data:
                file_position: int = self.seek_position(file_object.file_name)
                f.seek(file_position)

            if self._raw_data:
                catalog: str = file_path.parts[-2]
                match_rphost = RePatterns.re_rphost.match(catalog)
                if match_rphost:
                    rphost: int = match_rphost.groups()[0]
            
            skip_file: bool = False
            previous_event_pos: int = f.tell()
            lines: List[str] = []
            while True:
                for _ in range(1000):
                    pos_before_read = f.tell()
                    file_line = f.readline()
                    if not file_line:
                        break
                    match_new_line: Match[str] = RePatterns.re_new_event.match(file_line)
                    if match_new_line:
                        next_event_begin_pos: int = pos_before_read
                        # if self._tech_log_period.filter_time:
                        groups = match_new_line.groups()
                        next_event_time_str = file_path.stem + groups[0] + groups[1] + groups[2]
                        
                        if lines:
                            break
                        else:
                            event_time_str = next_event_time_str
                    else:
                        lines.append(file_line)
                if lines:
                    event_time = datetime.strptime(event_time_str, TimePatterns.template_time_full)
                    if self._tech_log_period.filter_time:
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
                    
                    
                    if event_line:
                        if self.filter_time(event_time) == 0:
                            tech_log_event = TechLogEvent(
                                                line=event_line,
                                                time=event_time,
                                                file_path=file_path,
                                                rphost=rphost,
                                                file_pos=previous_event_pos,
                                                event_len=len(event_line)
                                                )
                            self.execute_handlers(tech_log_event)

                        previous_event_pos = next_event_begin_pos
                if not file_line or skip_file:
                    break
                lines = [file_line]
                event_time_str = next_event_time_str


def main():
    f = LogReaderBase('test', './logs_test/rphost_1234')

    f.main_process()
    pass

if __name__ == '__main__':
    main()