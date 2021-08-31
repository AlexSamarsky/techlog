from functools import lru_cache
import re
from datetime import datetime
from LogDataclasses import EventProcessAnalyze, TechLogEvent, TechLogFile
from LogReader import LogReaderStream
from LogBase import LogBase

class LogEventAnalyze(LogBase):
    pass

    def __init__(self, name: str, begin_event_name: str, end_event_name) -> None:
        super().__init__(name)
        self._begin_event_name = begin_event_name
        self._end_event_name = end_event_name
        self._list_event_process_analyze = []
    
    @lru_cache(maxsize=None)
    def get_event_process_analyze(self, file_name: str) -> EventProcessAnalyze:
        event_process_analyze = EventProcessAnalyze(file_name)
        self._list_event_process_analyze.append(event_process_analyze)
        return event_process_analyze

    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        event_process_analyze = self.get_event_process_analyze(log_event.event.file.full_path)
        if log_event.event.name == self._begin_event_name:
            event_process_analyze.start_time = log_event.event.time
            event_process_analyze.start_event_file_pos = log_event.event.file_pos
        elif log_event.event.name == self._end_event_name:
            if event_process_analyze.start_time:
                event_process_analyze.end_time = log_event.event.time
                time_delta = event_process_analyze.end_time - event_process_analyze.start_time
                event_process_analyze.duration = time_delta.seconds * 1_000_000 + time_delta.microseconds
                log_event.event.duration = event_process_analyze.duration
                log_event.event.time = event_process_analyze.end_time
                self.execute_handlers(process_path, log_event)

    # def init_stream(self, start_time: datetime) -> None:
    #     self.set_time(start_time=start_time, end_time=None)
    #     self._settings = {}
