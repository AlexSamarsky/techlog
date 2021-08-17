
from LogBase import LogBase
from LogDataclasses import TechLogEvent, TechLogPeriod, TechLogFile, RawLogProps, TimePatterns, RePatterns


class LogWriteToConsole(LogBase):

    def add_data(self, process_path: str, log_event: TechLogEvent):

        return f',process_path={process_path},filePath={log_event.event.file_path},filePos={log_event.event.file_pos}'

    def main_process(self, process_path: str, log_event: TechLogEvent):
        if log_event.text[-1] == '\n':
            print(log_event.event.time.strftime(TimePatterns.template_file_date)+log_event.text.strip('\n')+self.add_data(process_path,log_event))
        else:
            print(log_event.event.time.strftime(TimePatterns.template_file_date)+log_event.text+self.add_data(process_path,log_event))


class LogWriteToFile(LogWriteToConsole):

    def __init__(self, name: str, file_name: str) -> None:
        super().__init__(name)
        self._file_name = file_name
        

    def execute_begin(self) -> None:
        super().execute_begin()
        self._writer = open(self._file_name, 'w', encoding=self._encoding)

    def execute_end(self) -> None:
        super().execute_end()
        self._writer.close()

    def main_process(self, process_path: str, log_event: TechLogEvent):
        self._writer.write(log_event.event.time.strftime(TimePatterns.template_file_date)+log_event.text.strip('\n')+self.add_data(process_path,log_event)+'\n')
