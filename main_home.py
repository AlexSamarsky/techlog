from LogReaderVector import LogReaderBaseVector
from datetime import datetime

from LogWrite import LogWriteToCatalogByField, LogWriteToCatalogByMinute, LogWriteToConsole, LogWriteToFile
from LogFilter import LogFilterPattern
from LogReader import LogReaderBase, LogReaderStream

profile = True

def main():

    log_reader = LogReaderBaseVector('reader', 'logs/logs_big')
    # log_reader = LogReaderStream('test', 'logs', 'settings.json')
    # \\onyx-1c-ppo2\Logz\PPO_Store_FULL
    # f.raw_data = False
    # log_reader.set_time(datetime(2021, 4, 26, 10, 14, 0, 0), datetime(2021, 4, 26, 10, 30,  0, 0))

    # log_writer_file = LogWriteToCatalogByMinute('write_file', 'logs_test/home')
    # log_writer_console = LogWriteToConsole('console')
    log_writer_file = LogWriteToCatalogByField('write_file', 'logs_test/test/by_minute')
    # log_writer_file.field_name = 'SessionID'
    # log_writer_file.by_minute = True
    # log_writer_file.
    # log_filter = LogFilterPattern('callid', ',CallID=3408160')

    # log_reader.connect(log_writer_console)
    # log_reader.connect(log_writer_console)
    # log_reader.connect(log_filter)
    # log_filter.connect(log_writer_console)
    
    log_reader.connect(log_writer_file)

    log_writer_file.init_stream()

    log_reader.main()
    # log_reader.main()
    # log_reader.main()
    # log_reader.main()
    # log_reader.main()
    # log_reader.main()
    pass

if __name__ == '__main__':
    
    if profile:

        import cProfile, pstats
        profiler = cProfile.Profile()
        profiler.enable()
        
        main()

        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats('tottime')
        stats.print_stats(10)
    else:
        main()
