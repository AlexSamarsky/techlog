
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
from LogDataclasses import TechLogEvent, TechLogFile, RawLogProps, TimePatterns, RePatterns

class LogReaderBase(LogBase):
    
    def __init__(self, name: str, files_path: str) -> None:
        super().__init__(name)
        self._files_path: str = files_path
        self._files_array: List[TechLogFile] = []
        self._raw_data: bool = True
        self._file_process_alg = 'block'
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
                try:
                    for tech_log_event in self.process_file(process_path, file_object):
                        self.execute_handlers(process_path, tech_log_event)
                except FileNotFoundError:
                    pass
    
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

    def event_process(self, process_path: str, file_object: TechLogFile, len_arr: int, index: int, event_string: str) -> None:
        if index > 0 and index < len_arr - 1:
            print(event_string)
    
    def process_file(self, process_path: str, file_object: TechLogFile) -> None:
        p = Path(file_object.full_path)
        file_path: Path = p.absolute()
        time_in_file = file_path.stem
        date_hour = time_in_file
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
            
            if self._file_process_alg == 'numpy':
                skip_file = False
                size_read = 1_000
                text = ''
                pos_before_read = f.tell()
                cnt_reads = floor(p.stat().st_size / size_read) + 2
                cnt = 0
                vec_event_process = np.vectorize(self.event_process)
                for _ in range(cnt_reads):
                    read_text = f.read(size_read)
                    if read_text:
                        text += read_text
                        # read_text = RePatterns.re_new_event_sub.sub('<tlaline>\g<0>', read_text)
                        # arr = np.char.split(read_text, sep='<tlaline>').reshape(1)[0]
                        arr = RePatterns.re_new_event_findall.findall(text)
                        arr.insert(0, '')
                        indexes = np.r_[0:len(arr):1]
                        vec_event_process(process_path, file_object, len(arr), indexes, arr)
                        break
                pass
            elif self._file_process_alg == 'block':
                skip_file = False
                size_read = 1_000_000
                text = ''
                pos_before_read = f.tell()
                cnt_reads = floor(p.stat().st_size / size_read) + 2
                cnt = 0
                for _ in range(cnt_reads):
                    read_text = f.read(size_read)
                    # len_text = len(bytes(read_text, 'utf8'))
                    if read_text:
                        text += read_text
                        prev_match = None
                        for match in RePatterns.re_new_event.finditer(text):
                            if not prev_match:
                                prev_match = match
                                if match.start():
                                    pos_before_read += len(bytes(text[:match.start()], 'utf8'))
                                continue
                            cnt += 1
                            if self._cnt_on and cnt > 500:
                                print(f'{Fore.YELLOW}{Style.BRIGHT}ONLY {cnt-1} RAWS WAS PROCESSED{Style.RESET_ALL}')
                                skip_file = True
                                break
                            groups = prev_match.groups()
                            event_time_str = f"{date_hour[:8]}{''.join(groups[:3])}"
                            event_time = datetime.strptime(event_time_str, TimePatterns.format_time_full)
                            if self._tech_log_period.filter_time:
                                filter_time_result = self.filter_time(event_time)
                                if filter_time_result == -1:
                                    event_line = ''
                                elif filter_time_result == 0: 
                                    event_line = text[prev_match.start():match.start()]
                                elif filter_time_result == 1:
                                    skip_file = True
                                    event_line = ''
                                    break
                            else:
                                event_line = text[prev_match.start():match.start()]
                                
                            event_len = len(bytes(event_line, 'utf8'))
                            if event_line:
                                raw_log_event = RawLogProps(
                                                    time=event_time,
                                                    file=file_object,
                                                    file_pos=pos_before_read,
                                                    duration=int(groups[3]),
                                                    name=groups[4],
                                                    level=groups[5],
                                                    time_str=event_time_str
                                                    )
                                tech_log_event = TechLogEvent(
                                                    text=event_line,
                                                    event=raw_log_event,
                                                    event_len=event_len,
                                                    )
                                file_object.raw_position = tech_log_event.event.file_pos + tech_log_event.event_len
                                yield tech_log_event
                                # self._string_process_with_write(event_line)
                            prev_match = match
                            pos_before_read += event_len
                            
                        if skip_file:
                            break
                        if match and prev_match:
                            text = text[match.start():]
                        
                    else:
                        if text:
                            groups = match.groups()
                            event_time_str = f"{date_hour[:8]}{''.join(groups[:3])}"
                            event_time = datetime.strptime(event_time_str, TimePatterns.format_time_full)
                            if (datetime.now() - event_time).total_seconds() > 1:
                                if self._tech_log_period.filter_time:
                                    if self.filter_time(event_time) == 0:
                                        event_line = text
                                else:
                                    event_line = text
                                    
                                if event_line:
                                    event_len = len(bytes(event_line, 'utf8'))
                                    raw_log_event = RawLogProps(
                                                        time=event_time,
                                                        file=file_object,
                                                        file_pos=pos_before_read,
                                                        duration=int(groups[3]),
                                                        name=groups[4],
                                                        level=groups[5],
                                                        time_str=event_time_str
                                                        )
                                    tech_log_event = TechLogEvent(
                                                        text=event_line,
                                                        event=raw_log_event,
                                                        event_len=event_len,
                                                        )
                                    file_object.raw_position = tech_log_event.event.file_pos + tech_log_event.event_len
                                    yield tech_log_event
                        break
            
            else:
                skip_file: bool = False
                pos_before_read = f.tell()
                lines: List[str] = []
                raw_log_event: RawLogProps = None
                next_raw_log_event: RawLogProps = None
                file_line = ''
                cnt = 0
                while True:
                    for _ in range(1000):
                        pos_before_read += len(bytes(file_line, 'utf8'))
                        file_line = f.readline()
                        if not file_line:
                            break
                        match_new_line: Match[str] = RePatterns.re_new_event.match(file_line)
                        if match_new_line:
                            groups = match_new_line.groups()
                            next_event_time_str = f"{date_hour[:8]}{''.join(groups[:3])}"
                            next_event_time = datetime.strptime(next_event_time_str, TimePatterns.format_time_full)
                            next_raw_log_event = RawLogProps(
                                                time=next_event_time,
                                                file=file_object,
                                                file_pos=pos_before_read,
                                                duration=int(groups[3]),
                                                name=groups[4],
                                                level=groups[5],
                                                time_str=next_event_time_str
                                                )
                            
                            if lines:
                                break
                            else:
                                lines.append(file_line)
                                raw_log_event = next_raw_log_event
                                # event_time_str = next_event_time_str
                        elif next_raw_log_event:
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
                            cnt += 1
                            if self._cnt_on and cnt > 500:
                                skip_file = True
                                print(f'{Fore.YELLOW}{Style.BRIGHT}ONLY {cnt-1} RAWS WAS PROCESSED{Style.RESET_ALL}')
                                break
                            event_len=len(bytes(event_line, 'utf8'))
                            tech_log_event = TechLogEvent(
                                                text=event_line,
                                                event=raw_log_event,
                                                event_len=event_len,
                                                )
                            file_object.raw_position = tech_log_event.event.file_pos + tech_log_event.event_len
                            yield tech_log_event

                        raw_log_event = next_raw_log_event
                            # previous_event_pos = next_event_begin_pos
                    if not file_line or skip_file:
                        break
                    lines = [file_line]


class LogReaderStream(LogReaderBase):
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
        with open(self._settings_path) as f:
            self._settings: Any = json.load(f)
        if not self._settings:
            self._settings = {}
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
        self._files_array = []
        
    def execute_end(self) -> None:
        settings = {}
        for file_object in self._files_array:
            settings[file_object.full_path] = file_object.raw_position
        with open(self._settings_path, 'w') as f:
            json.dump(settings, f)
        pass

    def init_stream(self, start_time: datetime) -> None:
        self.set_time(start_time=start_time, end_time=None)
        self._settings = {}
        self.seek_files()
        process_path = 'init'
        if self._files_array:
            for file_object in self._files_array:
                file_object.raw_position = 0
                tech_log_event = next(self.process_file(process_path, file_object), None)
                if tech_log_event == None:
                    file_object.raw_position = Path(file_object.full_path).stat().st_size
        self.execute_end()