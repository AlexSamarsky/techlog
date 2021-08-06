from TechLog import TechLog
import old.test as t
import re


class TechLog_csv(TechLog):
    
    _pattern_prop = re.compile(r',([\w\d:]+)=([^,]+)', flags=re.ASCII)
    _pattern_prop_quotes = re.compile(r',([\w\d:]+)=(\'[^\']+\')', flags=re.ASCII)
    _pattern_prop_double = re.compile(r',([\w\d:]+)=(\"[^\"]+\")', flags=re.ASCII)
    _array_fields = []

    def file_process(self):
        self._array_fields = []
        return super().file_process()

    def string_process(self, line):
        # line_new = super().string_process(line)
        new_line = self.convert_to_object(line)
        
        return new_line

    def convert_to_object(self, line):
        new_line = line.strip("\n").replace('-',',',1)

        new_line = new_line.replace('\n', '\\n')
        # new_line += ','

        match_prop_double = self._pattern_prop_double.match(new_line)
        re.finditer
        if match_prop_double:
            for group in reversed(match_prop_double):
                # new_line = new_line[:group()]
                pass
        
        return new_line

if __name__ == '__main__':
    file_in_name = 'test.log'
    file_out_name = 'test_out.log'
    tl = TechLog_csv(file_in_name, file_out_name)
    tl.file_process()
    