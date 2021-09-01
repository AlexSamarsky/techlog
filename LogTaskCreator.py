from LogDataclasses import TechLogEvent
import os
import re
from typing import List

from LogWrite import LogWriteToFile
from LogBase import LogBase


class TechLogTaskCreator(LogWriteToFile):
    
    def main_process(self, process_path: str, log_event: TechLogEvent) -> None:
        return super().main_process(process_path, log_event)