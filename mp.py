import multiprocessing
import time, random

def worker(input, output):
    """Функция, выполняемая рабочими процессами"""
    for func, args in iter(input.get, 'STOP'):
        result = calculate(func, args)
        output.put(result)

def calculate(func, args):
    """Функция, используемая для вычисления результата"""
    proc_name = multiprocessing.current_process().name
    result = func(*args)
    return f'{proc_name}, результат функции {func.__name__}{args} = {result}' 

########################################
# Функции, на которые ссылаются задачи #
########################################
def mul(a, b):
    time.sleep(0.5*random.random())
    return a * b

def plus(a, b):
    time.sleep(0.5*random.random())
    return a + b

def test():
    NUMBER_OF_PROCESSES = 4
    TASKS1 = [(mul, (i, 7)) for i in range(20)]
    TASKS2 = [(plus, (i, 8)) for i in range(10)]

    # Создание очередей
    task_queue = multiprocessing.Queue()
    done_queue = multiprocessing.Queue()

    # Заполнение очереди заданий
    for task in TASKS1:
        task_queue.put(task)

    # Запуск рабочих процессов
    for i in range(NUMBER_OF_PROCESSES):
        multiprocessing.Process(target=worker, args=(task_queue, done_queue)).start()

    # Получение и печать результатов
    print('НЕУПОРЯДОЧЕННЫЕ РЕЗУЛЬТАТЫ:\n')
    print('TASKS1:\n')
    for i in range(len(TASKS1)):
        print('\t', done_queue.get())

    # Добавляем больше задач с помощью метода `put()`
    for task in TASKS2:
        task_queue.put(task)

    # Выводим еще несколько результатов
    print('TASKS2:\n')
    for i in range(len(TASKS2)):
        print('\t', done_queue.get())

    # Говорим дочерним процессам остановиться
    print('STOP.')
    for i in range(NUMBER_OF_PROCESSES):
        task_queue.put('STOP')

if __name__ == '__main__':
    test()