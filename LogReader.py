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
from LogReaderVector import LogReaderBaseVector
from LogDataclasses import TechLogFile

class LogReaderStream(LogReaderBaseVector):
    pass

    def __init__(self, name: str, files_path: str, settings_path: str) -> None:
        super().__init__(name, files_path)
        self._settings_path: str = settings_path
        self._cache_len_minute = 5
    
    @property
    def cache_len_minute(self) -> int:
        return self._cache_len_minute

    @cache_len_minute.setter
    def cache_len_minute(self, minute: int) -> None:
        self._cache_len_minute = minute

    def execute_begin(self) -> None:
        try:
            with open(self._settings_path) as f:
                self._settings: Any = json.load(f)
            if not self._settings:
                self._settings = {}
        except:
            self._settings = {}
        if not self._tech_log_period:
            self.set_time(end_time=datetime.now() - timedelta(seconds=1))
        # self._settings = {}
        
    def seek_files(self) -> List[TechLogFile]:
        super().seek_files()
        for file_object in self._files_array:
            if self._cache_len_minute:
                file_object.stem[:8]
                pass
            raw_position: int = self._settings.get(file_object.full_path)
            if raw_position is None:
                self._settings[file_object.full_path] = raw_position
            else:
                file_object.raw_position = raw_position
        # self._files_array = []
        
    def execute_end(self) -> None:
        settings = {}
        for file_object in self._files_array:
            settings[file_object.full_path] = file_object.raw_position
        with open(self._settings_path, 'w') as f:
            json.dump(settings, f)
        pass

    def init_stream(self, start_time: datetime = None) -> None:
        self._settings = {}
        if start_time:
            self.set_time(start_time=start_time, end_time=None)
            if self._raw_data:
                self.seek_files()
                process_path = 'init'
                if self._files_array:
                    for file_object in self._files_array:
                        if self._tech_log_period.filter_time and self._raw_data and self.filter_time(Path(file_object.full_path).stem) == -1:
                            file_object.raw_position = Path(file_object.full_path).stat().st_size
                        else:
                            file_object.raw_position = self.seek_position(file_object.full_path, 0)
                        # tech_log_event = next(self.process_file(process_path, file_object), None)
                        # if tech_log_event == None:
                            # file_object.raw_position = Path(file_object.full_path).stat().st_size
        self.execute_end()