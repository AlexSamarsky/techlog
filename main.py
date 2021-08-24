from datetime import datetime, timedelta

from LogWrite import LogWriteToCatalogByMinute, LogWriteToConsole, LogWriteToFile
from LogFilter import LogFilterPattern
from LogReader import LogReaderBase, LogReaderStream

profile = True

def main():

    # log_reader = LogReaderBase('bill', '//app-bill-nord/logz_full/Logs_full')
    # log_reader = LogReaderBase('test', '//onyx-1c-ppo2/Logz/PPO_Store_FULL')
    # log_reader = LogReaderStream('test', 'logs', 'settings.json')
    # log_reader = LogReaderStream('bill', '//app-bill-nord/logz_full/Logs_full', 'settings.json')
    log_reader = LogReaderStream('bill', 'logs/Logs_full/', 'settings.json')
    
    # \\onyx-1c-ppo2\Logz\PPO_Store_FULL
    # f.raw_data = False
    log_reader.set_time(datetime(2021, 8, 23, 11, 0, 0, 0), datetime(2021, 8, 23, 11, 5,  0, 0))

    # log_writer_file = LogWriteToFile('write_file', 'logs_test/bill/test_out.log')
    # log_writer_console = LogWriteToConsole('console')
    log_writer_by_minute = LogWriteToCatalogByMinute('write_by_minute', 'logs_test/bill/by_minute')
    # log_writer_bill = LogWriteToCatalogByMinute('write_by_minute', 'logs_test/bill/7_20')
    # log_filter = LogFilterPattern('callid', ',CallID=3408160')

    # log_reader.connect(log_writer_console)
    # log_reader.connect(log_writer_console)
    # log_reader.connect(log_filter)
    # log_filter.connect(log_writer_console)
    
    log_reader.connect(log_writer_by_minute)

    # log_reader.init_stream(datetime.now() - timedelta(seconds=10))
    # log_reader.init_stream(datetime(2021, 8, 23, 12, 10, 0, 0))
    log_reader.main()
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
