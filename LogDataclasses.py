from dataclasses import dataclass, field
from datetime import datetime
from io import TextIOWrapper
import re

@dataclass
class TechLogFile:
    full_path: str
    file_name: str = ''
    raw_position: int = -1
    rel_path: str = ''
    file_io: TextIOWrapper = None
    stem: str = ''
    init_path: str = ''


@dataclass(frozen=True)
class RawLogProps:
    file: TechLogFile
    file_pos: int
    duration: int
    name: str
    level: int
    time_str: str = ''
    time: datetime = None

@dataclass(frozen=True)
class LogEvent:
    text: str
    event_len: int
    event: RawLogProps
   
@dataclass(frozen=True)
class TechLogEvent(LogEvent):
    rphost: int = 0
 
@dataclass
class LogObject:
    event: TechLogEvent

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

        self.start_time = start_time
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
        
            
    
class RePatterns:
    re_rphost = re.compile(r'rphost_([\d]+)')
    re_new_event = re.compile(r"^(\d{2,12}):(\d{2})\.(\d{6})-(\d+),(\w+),(\d+),", flags=re.MULTILINE)

class TimePatterns:
    format_time: str = "%y%m%d%H%M%S"
    format_time_full: str = "%y%m%d%H%M%S%f"
    format_date_hour: str = "%y%m%d%H"
    format_time_minute: str = "%y%m%d%H%M"
