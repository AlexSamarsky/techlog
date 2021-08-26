import re
from typing import List

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

class LogFilterByField(LogBase):

    def __init__(self, name: str, field_name: str, field_values: List[str]):
        super().__init__(name)
        self._field_name = field_name
        self._pattern = re.compile(f',{field_name}=([^,\'\"]+|\'[^\']+\'|\"[^\"]+\")', flags=re.ASCII | re.MULTILINE)
        if isinstance(field_values) == str:
            self._field_values = [field_values]
        elif isinstance(field_values) == List:
            self._field_values = field_values

    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        search = self._pattern.search(log_event.text)
        if search:
            field_value = search.group(1)
            if field_value in self._field_values:
                self.execute_handlers(process_path, log_event)
