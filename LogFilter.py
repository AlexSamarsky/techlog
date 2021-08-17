import re

from LogBase import LogBase
from LogDataclasses import TechLogEvent


class LogFilterPattern(LogBase):

    def __init__(self, name: str, filter: str) -> None:
        super().__init__(name)
        self._pattern = re.compile(filter)

    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        if self._pattern.search(log_event.text):
            self.execute_handlers(process_path, log_event)

class LogFilterDuration(LogBase):

    def __init__(self, name: str, duration: int) -> None:
        super().__init__(name)
        self._duration = duration

    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        if log_event.event.duration > self._duration:
            self.execute_handlers(process_path, log_event)
