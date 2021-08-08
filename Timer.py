import time

class Timer:
    _time = 0
    _time_start = 0
    _time_end = 0
    _timer_name = ''
    _started = False
    
    def __init__(self, name):
        self._time = 0
        self._timer_name = name
    
    def __str__(self):
        return f'timer {self._timer_name}: {self._time}'
        
    def start(self):
        self._time_start = time.time()
        self._started = True
    
    def stop(self):
        if self._started:
            self._time_end = time.time()
            self._time += self._time_end - self._time_start
            self._started = False