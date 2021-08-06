import multiprocessing as mp
import os
import re
import time
import random

filename = 'test.log'
pattern_start_record = re.compile(r"^\d{2}:\d{2}.\d{6}-\d+,")

def worker(input, output):
    """Функция, выполняемая рабочими процессами"""
    for line in iter(input.get, 'STOP'):
        result = process(line)
        output.put(result)

def process_wrapper(line):
    process(line)
    # with open(filename) as f:
        # f.seek(chunkStart)
        # lines = f.read(chunkSize).splitlines()
        # for line in lines:
            # process(line)


def chunkify(fname, size=1024*1024):
    fileEnd = os.path.getsize(fname)
    with open(fname, 'r', encoding="utf-8-sig", errors='replace') as f:
        record_data = ''
        while True:
            line = f.readline()
            group = pattern_start_record.match(line)
            if group:
                if record_data:
                    yield record_data
                record_data = line
            else:
                if not line:
                    yield record_data
                    yield ''
                    yield ''
                record_data += line
                # break


def process(line):
    # time.sleep(0.5*random.random())
    proc_name = mp.current_process().name
    return f'{proc_name} / {line[:50]}'


def test():
    NUMBERS_CORES = 2
    NUMBERS_QUEUE = NUMBERS_CORES * 500
    task_queue = mp.Queue(NUMBERS_QUEUE)
    done_queue = mp.Queue(NUMBERS_QUEUE)
    # pool = mp.Pool(NUMBERS_CORES)
    # with mp.Pool(NUMBERS_CORES) as pool:
    # jobs = []
    # cnt = 0
    file_read_iterator = chunkify(filename)
    many_rows = True
    for i in range(NUMBERS_QUEUE):
        line = next(file_read_iterator)
            
        if not line:
            many_rows = False
            task_queue.put('STOP')
            break
        task_queue.put(line)
    
    cnt = task_queue.qsize() - 1
    
    procs_arr = []
    for i in range(NUMBERS_CORES):
        proc = mp.Process(target=worker, args=(task_queue, done_queue))
        procs_arr.append(proc)
        proc.start()
    
    if many_rows:
        while True:
            result = done_queue.get(timeout=1)
            print('\t', result)
            line = next(file_read_iterator)
            if not line:
                task_queue.put('STOP')
                break
            task_queue.put(line)

    while cnt:
        result = done_queue.get(timeout=1)
        print('\t', result)
        task_queue.put('STOP')
        cnt -= 1
    
    for proc in procs_arr:
        proc.join()
        proc.close()

if __name__ == '__main__':
    test()