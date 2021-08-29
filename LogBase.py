import os
from pathlib import Path
from typing import List, Match
import re
from datetime import datetime
from math import floor
from Timer import Timer

from LogDataclasses import TechLogEvent, TechLogPeriod, TechLogFile, RawLogProps, TimePatterns, RePatterns

class LogBase():
    
    def __init__(self, name: str) -> None:
        self._handlers: List[LogBase] = []
        self._name: str = name
        self._handlers_len: int = len(self._handlers)
        self._path: str = ''
        self._event_object: TechLogEvent = None
        self._tech_log_period: TechLogPeriod = TechLogPeriod()
        self._encoding: str = "utf-8-sig"
        self._parent = None
        
    @property
    def path(self) -> str:
        return self._path
    
    @property
    def name(self) -> str:
        return self._name

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
        handler._parent = self
    
    def main(self):
        timer = Timer('test')
        timer.start()
        # self.execute_begin()
        self.execute_begin_handlers()
        self.main_process(self.name)
        # self.execute_end()
        self.execute_end_handlers()
        timer.stop()
        print(timer)

    def main_process(self, process_path: str) -> None:
        self.execute_handlers(process_path, None)
        pass

    def execute_begin_handlers(self) -> None:
        self.execute_begin()
        for h in self._handlers:
            h.execute_begin_handlers()        

    def execute_begin(self) -> None:
        pass

    def execute_end_handlers(self) -> None:
        self.execute_end()
        for h in self._handlers:
            h.execute_end_handlers()        

    def execute_end(self) -> None:
        pass

    def execute_handlers(self, process_path: str, event_object: TechLogEvent) -> None:
        for h in self._handlers:
            h.main_process(process_path + '/' + h.name, event_object)        
    
    def process(self) -> bool:
        return True

    def init(self) -> None:
        self.execute_init_handlers()
    
    def execute_init(self) -> None:
        pass

    def execute_init_handlers(self) -> None:
        self.execute_init()
        for h in self._handlers:
            h.execute_init_handlers()

    def execute_close_file(self) -> None:
        pass

    def execute_close_handlers(self) -> None:
        self.execute_close_file
        for h in self._handlers:
            h.execute_close_handlers()

    def seek_files(self) -> List[TechLogFile]:
        if not self._files_path:
            return
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
