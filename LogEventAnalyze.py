import re

from LogDataclasses import TechLogEvent
from LogBase import LogBase

class EventAnalyze(LogBase):
    pass

    def __init__(self, name: str, begin_event_name: str, end_event_name) -> None:
        super().__init__(name)
        self._begin_event_name = begin_event_name
        self._end_event_name = end_event_name
        self._begin_re = re.compile(f'{begin_event_name}')
        self._end_re = re.compile(f'{end_event_name}')
    
    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        if self._begin_re.search(log_event.text):
            self._begin_event_time = log_event.event.time
        elif self._end_re.search(log_event.text):
            if self._begin_event_time:
                self._end_event_time = log_event.event.time
                self._duration = self._end_event_time - self._begin_event_time
                self.execute_handlers(process_path, log_event)

