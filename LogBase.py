import os
from pathlib import Path
from typing import List
from datetime import datetime

from LogDataclasses import TechLogEvent, TechLogPeriod, TechLogFile

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
        self._path_to_files: str = ''
        self._files_array = None
        self._files_path = ''

    def get_str(self, level=0) -> str:
        self_name = '- '*level + self.__class__.__name__ + '/' + self._name
        for handler in self._handlers:
            self_name += '\n' + handler.get_str(level+1)
        return self_name

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

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, handler):
        self._parent = handler

    def set_time(self, start_time: datetime = None, end_time: datetime = None) -> None:
        self._tech_log_period = TechLogPeriod(start_time, end_time)

    def filter_time(self, event_time: str) -> int:
        if self._tech_log_period.filter_time:
            if isinstance(event_time, str):
                len_event: int = len(event_time)
                if (self._tech_log_period.start_time_str \
                        and event_time < self._tech_log_period.start_time_str[:len_event]):
                    return -1
                if (self._tech_log_period.end_time_str \
                        and event_time > self._tech_log_period.end_time_str[:len_event]):
                    return 1
            elif isinstance(event_time, datetime):
                if (self._tech_log_period.start_time \
                        and event_time < self._tech_log_period.start_time):
                    return -1
                if (self._tech_log_period.end_time \
                        and event_time > self._tech_log_period.end_time):
                    return 1
        return 0

    def connect(self, handler) -> None:
        self._handlers.append(handler)
        self._handlers_len = len(self._handlers)
        handler.parent = self

    def main(self):
        self.execute_begin_handlers()
        self.main_process(self.name)
        self.execute_end_handlers()

    def main_process(self, process_path: str, log_event: TechLogEvent = None) -> None:
        self.execute_handlers(process_path, log_event)

    def execute_begin_handlers(self) -> None:
        self.execute_begin()
        for hangler in self._handlers:
            hangler.execute_begin_handlers()

    def execute_begin(self) -> None:
        pass

    def execute_end_handlers(self) -> None:
        self.execute_end()
        for handler in self._handlers:
            handler.execute_end_handlers()

    def execute_end(self) -> None:
        pass

    def execute_handlers(self, process_path: str, log_event: TechLogEvent) -> None:
        for handler in self._handlers:
            handler.main_process(process_path + '_$_' + handler.name, log_event)

    def process(self) -> bool:
        pass

    def init(self) -> None:
        self.execute_init_handlers()

    def execute_init(self) -> None:
        pass

    def execute_init_handlers(self) -> None:
        self.execute_init()
        for hangler in self._handlers:
            hangler.execute_init_handlers()

    def execute_end_process_file(self, file_object: TechLogFile) -> None:
        pass

    def execute_end_process_file_handlers(self, file_object: TechLogFile) -> None:
        self.execute_end_process_file(file_object)
        for hangler in self._handlers:
            hangler.execute_end_process_file_handlers(file_object)

    def execute_begin_process_file(self, file_object: TechLogFile) -> None:
        pass

    def execute_begin_process_file_handlers(self, file_object: TechLogFile) -> None:
        self.execute_begin_process_file(file_object)
        for handler in self._handlers:
            handler.execute_begin_process_file_handlers(file_object)

    def seek_files(self):
        if not self._files_path:
            return
        path_or_file: str = self._files_path
        self._files_array: List[TechLogFile] = []

        try:
            if os.path.isdir(path_or_file):
                file_p: Path = Path(path_or_file)
                for file in file_p.glob('**/*'):
                    if os.path.isfile(file):
                        path_list = file.parts[len(file_p.parts):-1]
                        if len(path_list):
                            rel_path = os.path.join(*path_list)
                        else:
                            rel_path = ''
                        self._files_array.append(TechLogFile(
                                                    full_path=str(file),
                                                    rel_path=rel_path,
                                                    init_path=path_or_file
                                                ))
            elif os.path.isfile(path_or_file):
                self._files_array.append(TechLogFile(full_path=path_or_file))
        except FileNotFoundError:
            pass
