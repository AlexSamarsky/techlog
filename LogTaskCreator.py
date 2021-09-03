import os
import re
from typing import List
import uuid
from pathlib import Path
import shutil

from LogReaderVector import LogReaderBaseVector
from LogDataclasses import TechLogEvent
from LogWrite import LogWriteToCatalogByField, LogWriteToFile
from LogBase import LogBase


class TechLogTaskCreate(LogWriteToFile):
    
    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        return super().main_process(process_path, log_event)


class TechLogTaskProcess(LogBase):
    
    def __init__(self, name: str, path_tasks, task_uuid, field_start_time, field_end_time, field_path, field_rel_path) -> None:
        super().__init__(name)
        self._field_start_time = field_start_time
        self._field_end_time = field_end_time
        self._field_path = field_path
        self._field_rel_path = field_rel_path
        self._path_tasks = path_tasks
        self._task_uuid = task_uuid
    
    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        
        path_read = os.path.join(log_event.fields_values[self._field_path], log_event.fields_values[self._field_rel_path])
        log_reader = LogReaderBaseVector('task_read', path_read)
        log_reader.set_time(log_event.fields_values[self._field_start_time], log_event.fields_values[self._field_end_time])
        
        path_write = os.path.join(self._path_tasks, log_event.fields_values[self._task_uuid])
        
        log_writer = LogWriteToCatalogByField('task_write', path_write)
        # log_writer.init_stream()
        
        log_reader.connect(log_writer)
        
        log_reader.main()
        
        super().main_process(process_path, log_event)

    def init_stream(self) -> None:
        if not self._path_tasks:
            return
        p = Path(self._path_tasks)
        if not p.exists() and not p.suffix:
            p.mkdir(exist_ok=True, parents=True)

        if p.is_dir():
            for files in os.listdir(self._path_tasks):
                path = os.path.join(self._path_tasks, files)
                try:
                    shutil.rmtree(path)
                except OSError:
                    os.remove(path)
        else:
            if p.exists():
                os.remove(p)
