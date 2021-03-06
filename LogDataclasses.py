from dataclasses import dataclass, field
from datetime import datetime
from io import TextIOWrapper
import re
from typing import Iterator, List, Match

class RePatterns:
    re_rphost = re.compile(r'rphost_([\d]+)')
    
    re_tech_log_props = re.compile(r',([^,=\s]+)=([^,\'\"]+|\'[^\']+\'|\"[^\"]+\")', flags=re.MULTILINE | re.S)
    re_header_props = re.compile(r'([^,=\s]+):\s(?:(?=(.+)(\n|\Z)))', flags=re.MULTILINE | re.S)
    re_new_event = re.compile(r"^(\d{2,10}):(\d{2})\.(\d{4,6})-(\d+),(\w+),(\d+),", flags=re.MULTILINE)
    re_new_event_sub = re.compile(r"^(\d{2,10}:\d{2}\.\d{4,6}-\d+,\w+,\d+,)", flags=re.MULTILINE)
    # re_new_event_findall = re.compile(r"((\d{2,10}):(\d{2})\.(\d{4,6})-(\d+),(\w+),(\d+),.+?(?:^(?=\d{2}:\d{2}.\d{4,6})|\Z))", flags=re.MULTILINE | re.S)
    # re_new_event_findall = re.compile(r"((\d{2,10}):(\d{2})\.(\d{6})-(\d+),(\w+),(\d+),.+?(?:^(?=\d{2}:\d{2}\.\d{6})))", flags=re.MULTILINE | re.S)
    # re_new_event_findall_last = re.compile(r"((\d{2,10}):(\d{2})\.(\d{6})-(\d+),(\w+),(\d+),.+)", flags=re.MULTILINE | re.S)

    # re_new_event_findall = re.compile(r"(.+?(?:^(?=\d{2}:\d{2}.)|\Z))", flags=re.MULTILINE | re.S)
    re_new_event_line = re.compile(r"^(\d{2,10}):(\d{2})\.(\d{4,6})-(\d+),(\w+),(\d+),")


    re_new_event_findall = re.compile(r"(\d{2,10}):(\d{2})\.(\d{4,6})-(\d+),(\w+),(\d+),.+?(?:(?=\d{2,10}:\d{2}\.\d{4,6}-))", flags=re.MULTILINE | re.S)
    re_new_event_findall_last = re.compile(r"(\d{2,10}):(\d{2})\.(\d{4,6})-(\d+),(\w+),(\d+),.+", flags=re.MULTILINE | re.S)


class TimePatterns:
    format_time: str = "%y%m%d%H%M%S"
    format_time_full: str = "%y%m%d%H%M%S%f"
    format_date_hour: str = "%y%m%d%H"
    format_time_minute: str = "%y%m%d%H%M"


@dataclass
class TechLogPeriod:
    start_time: datetime = None
    end_time: datetime = None

    start_time_str: str = None
    end_time_str: str = None
    filter_time: bool = False

    def __init__(self, start_time: datetime = None, end_time: datetime = None):

        if end_time and start_time and end_time < start_time:
            raise ValueError('End time can\'t be lower than start time')
        
        if isinstance(start_time, str):
            self.start_time = datetime.strptime(start_time, TimePatterns.format_time_full)
        else:
            self.start_time = start_time
        if isinstance(end_time, str):
            self.end_time = datetime.strptime(end_time, TimePatterns.format_time_full)
        else:
            self.end_time = end_time
    
        if self.start_time or self.end_time:
            self.filter_time = True
            if self.start_time:
                self.start_time_str = self.start_time.strftime(TimePatterns.format_time_full)

            if self.end_time:
                self.end_time_str = self.end_time.strftime(TimePatterns.format_time_full)
            else:
                self.end_time = datetime(9999, 12, 30)
                self.end_time_str = '999999999999'
        else:
            self.filter_time = False
            return


@dataclass
class TechLogFile:
    full_path: str
    file_name: str = ''
    raw_position: int = -1
    rel_path: str = ''
    file_io: TextIOWrapper = None
    stem: str = ''
    init_path: str = ''
    skip_file: bool = False
    date_hour_str: str = ''
    date_hour: datetime = None
    # file_process_object: FileProcessObject = None
    # event_process_object: EventsProcessObject = None
    # files_array: List(TechLogWriteFile) = None


@dataclass
class TechLogWriteFile:
    full_path: str
    file_io: TextIOWrapper
    tech_log_file: TechLogFile


# @dataclass()
# class RawLogProps:


@dataclass()
class LogEvent:
    text: str = ''
    event_len: int = 0
    file: TechLogFile = None
    file_pos: int = 0
    duration: int = 0
    name: str = ''
    level: int = 0
    time_str: str = ''
    time: datetime = None
    fields_values: object = None


@dataclass()
class TechLogEvent(LogEvent):
    rphost: str = ''


@dataclass
class LogObject:
    event: TechLogEvent


@dataclass
class EventsProcessObject:
    skip_group: bool = False
    process_path: str = ''
    len_arr: int = 0
    text: str = ''
    len_text:int = 0
    # event_iter: Iterator[Match] = None
    current_pos: int = 0
    # current_pos_bytes: int = 0
    event_count: int = 0
    # event_previous: TechLogEvent = None
    tech_log_event: TechLogEvent = None
    # f = None

@dataclass
class EventProcessAnalyze:
    name: str
    obj: object = None
    start_event: str = ''
    start_time: datetime = None
    end_time: datetime = None
    duration: int = None
    start_event_file_pos: int = None
    tech_log_event: TechLogEvent = None


