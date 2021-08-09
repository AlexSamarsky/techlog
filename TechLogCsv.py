import re
import os
import shutil

from TechLogFile import TechLogFile
from Timer import Timer

profile = False

class TechLogCsv(TechLogFile):
    
    _pattern_all_props = re.compile(r',([^,=\s]+)=([^,\'\"]+|\'[^\']+\'|\"[^\"]+\")', flags=re.ASCII | re.IGNORECASE)
    _pattern_four_props = re.compile(r'([^,]+)-([^,]+),([^,]+),([^,]+)', flags=re.ASCII)
    _array_fields = ['_time', '_duration', '_event', '_level']
    _file_field_filter_name = ''
    _array_field_filter = []
    _main_timer_csv = None
    _new_alg = False
    
    
    
    def __init__(self, file_in_name, file_out_name, file_field_filter_name):
        super().__init__(file_in_name, file_out_name)
        self._file_field_filter_name = file_field_filter_name
        self._main_timer_csv = Timer('TechLogCsv')
    
    def get_field_index(self, string_array, name):
        for ind, field_name in enumerate(self._array_fields):
            if field_name == name:
                return ind
        return None
    
    def get_array_value(self, string_array, name):
        for ind, field_name in enumerate(self._array_fields):
            if field_name == name:
                return string_array[ind]
        return None
    
    def main_process(self):
        
        self._main_timer_csv.start()
        
        self._array_fields = ['_time', '_duration', '_event', '_level']

        # csv_temp - шапка файла, temp - файл куда
        dirname = os.path.dirname(self._file_out_name)
        if dirname:
            dirname += '/'
        filename = os.path.basename(self._file_out_name)
        file_out_main_name = self._file_out_name
        file_out_csv_temp_name = f'{dirname}temp_csv_{filename}'
        self._file_out_name = f'{dirname}temp_{filename}'

        with open(self._file_field_filter_name, "r", encoding=self._encoding, errors='replace') as file_field_filter:
            string_fields_filter = file_field_filter.readline()
            # string_pattern = f',({string_fields_filter.replace(",", "|")})=([^,\'\"]+|\'[^\']+\'|\"[^\"]+\")'
            # self._pattern_all_props = re.compile(string_pattern, flags=re.ASCII | re.IGNORECASE)
            # ,([^,=\s]+)=([^,\'\"]+|\'[^\']+\'|\"[^\"]+\")
            field_array = string_fields_filter.split(',')
            if self._new_alg:
                self._array_field_filter = []
                for field_name in field_array:
                    self._array_field_filter.append([field_name, None])
                self._array_field_filter.sort(key=lambda field: field[0])
            else:
                self._array_field_filter = field_array
            
        super().main_process()

        with open(file_out_csv_temp_name, 'w', encoding=self._encoding, errors='replace') as file_out_csv_temp:
            file_out_csv_temp.write(','.join(self._array_fields)+'\n')

        with open(file_out_main_name,'wb') as outfile:
            for i, fname in enumerate([file_out_csv_temp_name, self._file_out_name]):
                with open(fname, 'rb') as infile:
                    if i != 0:
                        str_v = infile.readline()  # Throw away header on all but first file
                        str_v = str_v.replace(b'\xef\xbb\xbf', b'', 1)
                        outfile.write(str_v)
                    shutil.copyfileobj(infile, outfile, 5*1024*1024)
        
        if os.path.exists(self._file_out_name):
            os.remove(self._file_out_name)
        if os.path.exists(file_out_csv_temp_name):
            os.remove(file_out_csv_temp_name)
        
        self._main_timer_csv.stop()
        print(self._main_timer_csv)
        
    def array_process(self, string_array):
        return string_array
    
    def string_process(self, line):
        new_line = super().string_process(line)

        string_array = self.convert_to_array(new_line)
        string_array = self.array_process(string_array)
        
        return self.convert_to_string(string_array)

    def field_value_process(self, field_name, field_value):
        return field_value
    
    def convert_to_array(self, line):
        
        if not line:
            return []

        new_line = line
        
        string_array = ["" for n in range(len(self._array_fields))]
        
        match_four_props = self._pattern_four_props.match(new_line)
        if match_four_props:
            string_array[:4] = match_four_props.groups()[:4]
            new_line = new_line[match_four_props.regs[0][1]:]
        
        props_found = self._pattern_all_props.findall(new_line)
        if props_found:
            if self._new_alg:
                props_found.sort(key=lambda x: x[0].lower())
                filter_field_ind = 0
                props_ind = 0
                filter_field_len = len(self._array_field_filter)
                props_len = len(props_found)
                filter_field_arr = self._array_field_filter[filter_field_ind]
                props_name = props_found[props_ind][0].lower()
                props_value = props_found[props_ind][1]
                for _ in range(props_len + filter_field_len):
                    if filter_field_arr[0] == props_name:
                        filter_field_ind += 1
                        props_ind += 1
                        props_value = self.field_value_process(props_name, props_value)
                        if not filter_field_arr[1]:
                            filter_field_arr[1] = len(string_array)
                            string_array.append(props_value)
                            self._array_fields.append(filter_field_arr[0])
                        else:
                            string_array[filter_field_arr[1]] = props_value
                        if props_ind == props_len or filter_field_ind == filter_field_len:
                            break
                        props_name = props_found[props_ind][0].lower()
                        props_value = props_found[props_ind][1]
                        filter_field_arr = self._array_field_filter[filter_field_ind]
                    elif filter_field_arr[0] > props_name:
                        props_ind += 1
                        if props_ind == props_len or filter_field_ind == filter_field_len:
                            break
                        props_name = props_found[props_ind][0].lower()
                        props_value = props_found[props_ind][1]
                    else:
                        filter_field_ind += 1
                        if props_ind == props_len or filter_field_ind == filter_field_len:
                            break
                        filter_field_arr = self._array_field_filter[filter_field_ind]
            else:
                    
                len_match_props = len(props_found)
                ind = 0
                for ind in range(len_match_props):
                    props_name, props_value = props_found[ind]
                    props_name = props_name.lower()
                    try:
                        index = self._array_fields.index(props_name)
                        props_value = self.field_value_process(props_name, props_value)
                        string_array[index] = props_value
                    except ValueError:
                        try:
                            _ = self._array_field_filter.index(props_name)
                            self._array_fields.append(props_name)
                            props_value = self.field_value_process(props_name, props_value)
                            string_array.append(props_value)
                        except ValueError:
                            pass
        return string_array
    
    def convert_to_string(self, string_array):
        return ','.join(string_array)

def main():
    file_in_name = 'logs/'
    file_in_name = 'logs_test/test.log'
    file_out_name = 'logs_test/test_out.csv'
    file_field_filter_name = 'fields.txt'
    tl = TechLogCsv(file_in_name, file_out_name, file_field_filter_name)
    tl.main_process()

if __name__ == '__main__':
    
    if profile:

        import cProfile, pstats
        profiler = cProfile.Profile()
        profiler.enable()
        
        main()

        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats('tottime')
        stats.print_stats()
    else:
        main()