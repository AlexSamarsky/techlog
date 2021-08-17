import os
from pathlib import Path
from typing import List, Match
import re
from datetime import datetime
from math import floor

from LogBase import LogBase
from LogDataclasses import TechLogEvent, TechLogFile, RawLogProps, TimePatterns, RePatterns

class LogReaderBase(LogBase):
    
    def __init__(self, name, files_path):
        super().__init__(name)
        self._files_path: str = files_path
        self._files_array: List[TechLogFile] = []
        self._raw_data: bool = True

    @property
    def raw_data(self) -> bool:
        return self._raw_data
    
    @raw_data.setter
    def raw_data(self, raw_data) -> None:
        self._raw_data = raw_data

    def __seek_files(self) -> List[str]:
        path_or_file: str = self._files_path
        self._files_path: List[TechLogFile] = []
        
        try:
            if os.path.isdir(path_or_file):
                p: Path = Path(path_or_file)
                for f in p.glob('**/*'):
                    if os.path.isfile(f):
                        path_list = f.parts[len(p.parts):-3]
                        if len(path_list):
                            rel_path = os.path.join(path_list)
                        self._files_path.append(TechLogFile(
                                                    file_name=str(f),
                                                    rel_path=rel_path,
                                                    
                                                ))
                        # self.__filter_file(str(f))
            elif os.path.isfile(path_or_file):
                self._files_path.append(TechLogFile(file_name=path_or_file))
                # self.__filter_file(path_or_file)
        except FileNotFoundError:
            pass

    def __filter_file(self, file_name) -> bool:
        if self._raw_data:
            if self.filter_time(Path(file_name).stem) != 0:
                return False

        return True
    
    def main_process(self, process_path: str) -> None:
        self.__seek_files()
        if self._files_path:
            for file_object in self._files_path:
                self.process_file(process_path, file_object)
    
    def seek_position(self, file_name, start_seek: int = None) -> int:

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
                if portion < 100_000:
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

    def process_file(self, process_path: str, file_object: TechLogFile) -> None:
        p = Path(file_object.file_name)
        file_path: Path = p.absolute()
        time_in_file = file_path.stem
        date_hour = time_in_file[:8]
        with open(file_object.file_name, 'r', encoding=self._encoding, errors='replace') as f:
            
            if self._tech_log_period.filter_time and not self.__filter_file(time_in_file):
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
            pos_before_read = f.tell()
            lines: List[str] = []
            raw_log_event = None
            file_line = ''
            while True:
                for _ in range(1000):
                    pos_before_read += len(file_line)
                    file_line = f.readline()
                    if not file_line:
                        break
                    match_new_line: Match[str] = RePatterns.re_new_event.match(file_line)
                    if match_new_line:
                        groups = match_new_line.groups()
                        next_event_time_str = f"{date_hour}{''.join(groups[:3])}"
                        next_event_time = datetime.strptime(next_event_time_str, TimePatterns.format_time_full)
                        next_raw_log_event = RawLogProps(
                                            time=next_event_time,
                                            file=file_object,
                                            file_pos=pos_before_read,
                                            duration=int(groups[3]),
                                            name=groups[4],
                                            level=groups[5]
                                            )
                        
                        if lines:
                            break
                        else:
                            lines.append(file_line)
                            raw_log_event = next_raw_log_event
                            # event_time_str = next_event_time_str
                    else:
                        lines.append(file_line)
                if lines:
                    event_time = raw_log_event.time
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
                        tech_log_event = TechLogEvent(
                                            text=event_line,
                                            event=raw_log_event,
                                            event_len=len(event_line)
                                            )
                        self.execute_handlers(process_path, tech_log_event)
                        raw_log_event = next_raw_log_event
                        # previous_event_pos = next_event_begin_pos
                if not file_line or skip_file:
                    break
                lines = [file_line]


