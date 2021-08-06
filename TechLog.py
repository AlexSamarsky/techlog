import os
import multiprocessing as mp
import re
import time


class TechLog:
    _pattern_start_record = re.compile(r"^\d{2}:\d{2}.\d{6}-\d+,")
    _file_in_name = ''
    _file_out_name = ''
    _NUMBERS_CORES = 2
    _NUMBERS_QUEUE = _NUMBERS_CORES * 250
    _multitreading = False
    _file_out_stream = None
    _encoding = "utf-8-sig"

    def __init__(self, file_in_name, file_out_name):
        self._file_in_name = file_in_name
        self._file_out_name = file_out_name
    
    def _string_process_with_write(self, line):
        new_line = self.string_process(line)
        self._write_stream(new_line)
  
    def string_process(self, line):
        # time.sleep(0.5*random.random())
        proc_name = mp.current_process().name
        return f'{proc_name} / {line[:50]} \n'

    def _write_stream(self, new_line):
        if not new_line.endswith('\n'):
            new_line += '\n'
        self._file_out_stream.write(new_line)

    def _worker_string_process(self, input, output):
        """Функция, выполняемая рабочими процессами"""
        for line in iter(input.get, 'STOP'):
            result = self._string_process_with_write(line)
            self._write_stream(result)
            output.put(result)

    def _chunkify(self):
        path_or_file = self._file_in_name
        files_arr = []
        if os.path.isdir(path_or_file):
            for adress, _, files in os.walk(path_or_file, topdown=False):
                if not adress.endswith('/'):
                    adress += '/'
                for file in files:
                    files_arr.append(adress + file)
        elif os.path.isfile(path_or_file):
            files_arr.append(path_or_file)
        
        for file_name in files_arr:
            with open(file_name, 'r', encoding=self._encoding, errors='replace') as f:
                record_data = ''
                while True:
                    line = f.readline()
                    group = self._pattern_start_record.match(line)
                    if group:
                        if record_data:
                            yield record_data
                        record_data = line
                    else:
                        if not line:
                            yield record_data
                            break
                        record_data += line
        yield ''
        yield ''

    def file_process(self):

        start_time = time.time()

        self._file_out_stream = open(self._file_out_name, 'w', encoding=self._encoding)

        if self._multitreading:
            task_queue = mp.Queue(self._NUMBERS_QUEUE)
            done_queue = mp.Queue(self._NUMBERS_QUEUE)
            file_read_iterator = self._chunkify()
            many_rows = True
            for i in range(self._NUMBERS_QUEUE):
                line = next(file_read_iterator)
                    
                if not line:
                    many_rows = False
                    task_queue.put('STOP')
                    break
                task_queue.put(line)
            
            cnt = task_queue.qsize() - 1
            
            procs_arr = []
            for i in range(self._NUMBERS_CORES):
                proc = mp.Process(target=self._worker_string_process, args=(task_queue, done_queue))
                procs_arr.append(proc)
                proc.start()
            
            if many_rows:
                while True:
                    result = done_queue.get(timeout=1)
                    # print('\t', result)
                    line = next(file_read_iterator)
                    if not line:
                        task_queue.put('STOP')
                        break
                    task_queue.put(line)

            while cnt:
                result = done_queue.get(timeout=1)
                # print('\t', result)
                cnt -= 1
            
            task_queue.put('STOP')
            # done_queue.get(timeout=1)

            for proc in procs_arr:
                proc.join()
                proc.close()
        else:
            file_read_iterator = self._chunkify()
            while True:
                line = next(file_read_iterator)
                self._string_process_with_write(line)
                if not line:
                    break
        
        finish_time = time.time()
        print(finish_time - start_time)


if __name__ == '__main__':
    file_in_name = 'test.log'
    file_out_name = 'test_out.log'
    tl = TechLog(file_in_name, file_out_name)
    tl.file_process()
    