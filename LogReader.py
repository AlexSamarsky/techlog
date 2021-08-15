from LogBase import LogBase


class LogTechReader(LogBase):
    
    def __init__(self, files_path):
        super().__init__()
        self._files_path = files_path
    
    def process(self):
        return True