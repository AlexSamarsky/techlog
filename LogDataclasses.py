from dataclasses import dataclass, field
from datetime import datetime
import re

@dataclass(frozen=True)
class LogEvent:
    line: str
    time: datetime
    file_path: str
    file_pos: int
    event_len: int
    
    # def __post_init__(self):
    #     self.event_len = len(self.line)
   
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
    
    def __post_init__(self):
        if self.start_time or self.end_time:
            self.filter_time = True
            if self.start_time:
                self.start_time_str = self.start_time.strftime(TimePatterns.template_time_full)

            if self.end_time:
                self.end_time = self.end_time.strftime(TimePatterns.template_time_full)
            else:
                self.end_time = datetime(9999, 12, 30)
                self.end_time_str = '999999999999'
        else:
            self.filter_time = False
            return
        
            
    
@dataclass
class TechLogFile:
    file_name: str
    raw_position: int = -1


class RePatterns:
    re_rphost = re.compile(r'rphost_([\d]+)')
    re_new_event = re.compile(r"^(\d{2,12}):(\d{2})\.(\d{6})-\d+,")

class TimePatterns:
    template_time: str = "%y%m%d%H%M%S"
    template_time_full: str = "%y%m%d%H%M%S%f"
