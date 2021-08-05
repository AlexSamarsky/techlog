import multiprocessing as mp
import os
import re
import time
import random

filename = 'testcopy.log'
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
    with open(fname, 'r') as f:
        # chunkEnd = f.tell()
        while True:
            # chunkStart = chunkEnd
            line = f.readline()
            if line:
                yield line
            else:
                yield ''
            # chunkEnd = f.tell()
            # yield chunkStart, chunkEnd - chunkStart
            # if not chunkSize:
                # break


def process(line):
    # time.sleep(0.5*random.random())
    proc_name = mp.current_process().name
    return f'{proc_name} / {line[:20]}'


def test():
    NUMBERS_QUEUE = 10
    NUMBERS_CORES = 1
    task_queue = mp.Queue(NUMBERS_QUEUE)
    done_queue = mp.Queue(NUMBERS_QUEUE)
    # pool = mp.Pool(NUMBERS_CORES)
    # with mp.Pool(NUMBERS_CORES) as pool:
    jobs = []
    cnt = 0
    file_read_iterator = chunkify(filename)
    for i in range(NUMBERS_QUEUE):
        line = next(file_read_iterator)
            
        if not line:
            task_queue.put('STOP')
            break
        task_queue.put(line)

    for i in range(NUMBERS_CORES):
        mp.Process(target=worker, args=(task_queue, done_queue)).start()

    time.sleep(1)
    
    while True:
        result = done_queue.get()
        print('\t', result)
        line = next(file_read_iterator)
        if not line:
            task_queue.put('STOP')
            break
        task_queue.put(line)

    for i in range(NUMBERS_QUEUE-1):
        result = done_queue.get()
        print('\t', result)
    
    # pass
    for i in range(NUMBERS_QUEUE):
        task_queue.put('STOP')
    # for line in iter(done_queue.get, 'STOP'):
    #     result = done_queue.get()
    #     print(result)
        

    # for chunkStart, chunkSize in chunkify(filename):
        # cnt+=1
        # process_wrapper(chunkStart, chunkSize) 
        # jobs.append(pool.apply_async(process_wrapper, (chunkStart, chunkSize)))
    # print(cnt)

    # pool.close()

if __name__ == '__main__':
    test()