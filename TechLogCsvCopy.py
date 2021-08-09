import re
import os
import shutil
import time
# import numpy as np

from TechLogFile import TechLogFile
from Timer import Timer

class TechLogCsv(TechLogFile):
    
    _pattern_all_props = re.compile(r',([^,=\s]+)=([^,\'\"]+|\'[^\']+\'|\"[^\"]+\")', flags=re.ASCII)
    _pattern_four_props = re.compile(r'([^,]+)-([^,]+),([^,]+),([^,]+)', flags=re.ASCII)
    # _pattern_prop = re.compile(r',(')
    _array_fields = ['_time', '_duration', '_event', '_level']
    _file_field_filter_name = ''
    _file_out_name_csv_temp = ''
    _array_field_filter = []
    _string_field_filter = ''
    _dict_field_filter = {}
    _file_out_name_main = ''
    _numpy_field_filter = None
    
    _timer1 = None
    _timer2 = None
    _timer3 = None
    _timer4 = None
    _timer5 = None
    _timer6 = None
    
    def __init__(self, file_in_name, file_out_name, file_field_filter_name):
        super().__init__(file_in_name, file_out_name)
        self._timer1 = Timer("timer1")
        self._timer2 = Timer("timer2")
        self._timer3 = Timer("timer3")
        self._timer4 = Timer("timer4")
        self._timer5 = Timer("timer5")
        self._timer6 = Timer("timer6")
        
        self._cum_time = 0
        self._cum2_time = 0
        self._file_field_filter_name = file_field_filter_name
    
    def get_array_value(self, string_array, name):
        index = self._array_fields.index(name)
        if index >= 0:
            return string_array[index]
        return None
    
    def main_process(self):
        
        start_time = time.time()
        
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
            self._string_field_filter = file_field_filter.readline()
            self._array_field_filter = self._string_field_filter.split(',')
            # self._dict_field_filter = {}
            # for field in self._array_field_filter:
                # self._dict_field_filter[field] = 0
            # self._numpy_field_filter = np.array(sorted(self._array_field_filter))
            # self._string_field_filter = f',{self._string_field_filter},'
            
        super().main_process()

        with open(file_out_csv_temp_name, 'w', encoding=self._encoding, errors='replace') as file_out_csv_temp:
            file_out_csv_temp.write(','.join(self._array_fields))
        
        with open(file_out_main_name,'wb') as wfd:
            for f in [file_out_csv_temp_name, self._file_out_name]:
                with open(f,'rb') as fd:
                    shutil.copyfileobj(fd, wfd)        
        
        if os.path.exists(self._file_out_name):
            os.remove(self._file_out_name)
        if os.path.exists(file_out_csv_temp_name):
            os.remove(file_out_csv_temp_name)
        
        finish_time = time.time()
        print(f'tech log csv: {finish_time - start_time}')

        print(self._timer1)
        print(self._timer2)
        print(self._timer3)
        print(self._timer4)
        print(self._timer5)
        print(self._timer6)
        # print(f'timer 2: {self._cum2_time}')

    def array_process(self, string_array):
        return string_array
    
    def string_process(self, line):
        new_line = super().string_process(line)

        string_array = self.convert_to_array(new_line)
        string_array = self.array_process(string_array)
        
        return self.convert_to_string(string_array)

    def get_prop(self, string_array, groups, ind):
        field_name, value = groups[ind]
        ind += 1
        try:
            index = self._array_fields.index(field_name)
            string_array[index] = value
        except ValueError:
            try:
                _ = self._array_field_filter.index(field_name)
                self._array_fields.append(field_name)
                string_array.append(value)
            except ValueError:
                pass
        return string_array
    
    def get_prop_value(self, groups, ind):
        field_name, value = groups[ind]
        index = -1
        try:
            index = self._array_fields.index(field_name)
        except ValueError:
            try:
                _ = self._array_field_filter.index(field_name)
                self._array_fields.append(field_name)
                index = len(self._array_fields) - 1
            except ValueError:
                pass
        return index, value
    
    def convert_to_array(self, line):
        
        self._timer1.start()
        new_line = line
        
        self._timer2.start()
        string_array = ["" for n in range(len(self._array_fields))]
        
        match_four_props = self._pattern_four_props.match(new_line)
        if match_four_props:
            string_array[:4] = match_four_props.groups()[:4]
            new_line = new_line[match_four_props.regs[0][1]:]
        self._timer2.stop()
        
        self._timer3.start()
        match_props = self._pattern_all_props.findall(new_line)
        self._timer3.stop()
        self._timer4.start()
        if match_props:
            len_match_props = len(match_props)
            ind = 0
            # string_array = self.get_prop(string_array, match_props, len_match_props, ind)
            
            # for group in match_props:
            # len_match_props = len(match_props)
            # ind = 0
            for ind in range(len_match_props):
            # while ind < len_match_props:
            #     self._timer5.start()
                # ind += 1
                # field_name = group[0].lower()
                # try:
                #     index = self._array_fields.index(field_name)
                #     string_array[index] = group[1]
                # except ValueError:
                #     ts2 = time.time()
                #     # ind = np.where(self._numpy_field_filter == field_name)
                #     # if len(ind[0]):
                #     if field_name in self._array_field_filter:
                #         self._array_fields.append(field_name)
                #         string_array.append(group[1])
                #     te2 = time.time()
                #     self._cum2_time += (te2-ts2)
                # self._timer5.start()
                # # string_array = self.get_prop(string_array, match_props, ind)
                # index, value = self.get_prop_value(match_props, ind)
                # self._timer5.stop()
                self._timer6.start()
                # if index >= 0:
                #     if index == len(string_array):
                #         string_array.append(value)
                #     else:
                #         string_array[index] = value
                # field_name, value = match_props[ind]
                # try:
                #     index = self._array_fields.index(field_name)
                #     string_array[index] = value
                # except ValueError:
                #     try:
                #         _ = self._array_field_filter.index(field_name)
                #         self._array_fields.append(field_name)
                #         string_array.append(value)
                #     except ValueError:
                #         pass
                self._timer6.stop()
            #     # try:
            #     #     index = self._array_fields.index(field_name)
            #     #     string_array[index] = group[1]
            #     # except ValueError:
            #     #     try:
            #     #         index_new = self._array_field_filter.index(field_name)
            #     #         self._array_fields.append(field_name)
            #     #         string_array.append(group[1])
            #     #     except:
            #     #         pass
            #     # if self._array_field_filter.index(group[0]) >= 0:
            #     # if re.search(f',({group[0]}),', self._string_field_filter):
            #     # if field_name in self._array_fields:
            #     #     ts = time.time()
            #     #     string_array[self._array_fields.index(field_name)] = group[1]
            #     #     te = time.time()
            #     #     self._cum_time += (te-ts)
            #     # else:
            #     #     if field_name in self._array_field_filter:
            #     #         self._array_fields.append(field_name)
            #     #         string_array.append(group[1])
            #     self._timer5.stop()
        self._timer4.stop()
        self._timer1.stop()
        
        return string_array
    
    def convert_to_string(self, string_array):
        return ','.join(string_array)

if __name__ == '__main__':
    

    # import cProfile, pstats
    # profiler = cProfile.Profile()
    # profiler.enable()
    
    
    file_in_name = 'logs/'
    # file_in_name = 'test.log'
    file_out_name = 'test_out.csv'
    file_field_filter_name = 'fields.txt'
    tl = TechLogCsv(file_in_name, file_out_name, file_field_filter_name)
    tl.main_process()


    # profiler.disable()
    # stats = pstats.Stats(profiler).sort_stats('cumtime')
    # stats.print_stats()