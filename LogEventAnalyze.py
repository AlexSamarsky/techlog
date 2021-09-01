from functools import lru_cache
import re
from datetime import datetime
from LogDataclasses import EventProcessAnalyze, TechLogEvent, TechLogFile, TimePatterns
from LogReader import LogReaderStream
from LogBase import LogBase

class LogEventAnalyze(LogBase):
    pass

    def __init__(self, name: str, begin_event_name: str, end_event_name: str, path: str) -> None:
        super().__init__(name)
        self._begin_event_name = begin_event_name
        self._end_event_name = end_event_name
        self._list_event_process_analyze = []
        self._path = path
    
    @lru_cache(maxsize=None)
    def get_event_process_analyze(self, file_name: str) -> EventProcessAnalyze:
        event_process_analyze = EventProcessAnalyze(file_name)
        event_process_analyze.tech_log_event = TechLogEvent(
                                                    time=datetime.now(),
                                                    file=None,
                                                    file_pos=0,
                                                    duration=0,
                                                    name='',
                                                    level='',
                                                    time_str='',
                                                    text='', 
                                                    # event=raw_log_event,
                                                    event_len=0,
                                                    rphost=None
                                                    )
        self._list_event_process_analyze.append(event_process_analyze)
        return event_process_analyze

    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        event_process_analyze = self.get_event_process_analyze(log_event.file.full_path)
        if not event_process_analyze.tech_log_event.file:
            event_process_analyze.tech_log_event.file = log_event.file
            
        if log_event.name == self._begin_event_name:
            event_process_analyze.start_time = log_event.time
            event_process_analyze.start_event_file_pos = log_event.file_pos
            event_process_analyze.tech_log_event.text = log_event.text
        elif log_event.name == self._end_event_name:
            if event_process_analyze.start_time:
                event_process_analyze.end_time = log_event.time
                time_delta = event_process_analyze.end_time - event_process_analyze.start_time
                event_process_analyze.duration = time_delta.seconds * 1_000_000 + time_delta.microseconds
                
                event_process_analyze.tech_log_event.duration = event_process_analyze.duration
                event_process_analyze.tech_log_event.time = event_process_analyze.end_time
                event_process_analyze.tech_log_event.text = event_process_analyze.tech_log_event.text.rstrip('\n') \
                                                            + f',tla:start_time={datetime.strftime(event_process_analyze.start_time, TimePatterns.format_time_full)}' \
                                                            + f',tla:end_time={datetime.strftime(event_process_analyze.end_time, TimePatterns.format_time_full)}' \
                                                            + f',tla:duration={event_process_analyze.tech_log_event.duration}' \
                                                            + f',tla:path={self._path}' \
                                                            + f',tla:rel_path={log_event.file.stem}' \
                                                            + '\n'
                self.execute_handlers(process_path, event_process_analyze.tech_log_event)

    # def init_stream(self, start_time: datetime) -> None:
    #     self.set_time(start_time=start_time, end_time=None)
    #     self._settings = {}

class LogEventExtract(LogBase):
    
    def main_process(self, process_path: str) -> None:
        pass