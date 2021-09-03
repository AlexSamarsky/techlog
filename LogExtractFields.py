

from LogDataclasses import RePatterns, TechLogEvent
from typing import List
from LogBase import LogBase


class TechLogExtractFields(LogBase):
    
    def __init__(self, name: str, fields_name) -> None:
        super().__init__(name)
        if isinstance(fields_name, str):
            self._fields = [fields_name]
        elif isinstance(fields_name, List):
            self._fields = fields_name

    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        
        search_props = RePatterns.re_tech_log_props.findall(log_event.text)
        if search_props:
            if not log_event.fields_values:
                log_event.fields_values = {}
            for i, prop in enumerate(search_props):
                if prop[0] in self._fields:
                    log_event.fields_values[prop[0]] = prop[1]
        
        super().main_process(process_path, log_event)