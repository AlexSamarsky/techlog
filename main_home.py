from LogReaderVector import LogReaderBaseVector
from datetime import datetime

from LogWrite import LogWriteToCatalogByField, LogWriteToConsole, LogWriteToFile
from LogFilter import LogFilterPattern
from LogReader import LogReaderBase, LogReaderStream

profile = True

def main():

    log_reader = LogReaderBaseVector('bill_test', 'logs_test/bill/main')
    log_reader.set_time(datetime(2021, 8, 27, 14, 20, 0, 0), datetime(2021, 8, 27, 14, 50,  0, 0))
    log_writer_by_minute = LogWriteToCatalogByField('write_by_minute', 'logs_test/bill/by_minute')
    log_writer_by_minute.add_data = False
    log_writer_by_minute.append_to_file = True
    # log_writer_by_minute.field_name = 'SessionID'
    log_writer_by_minute.by_minute = True
    
    
    log_reader.connect(log_writer_by_minute)

    # log_reader.init_stream(datetime.now() - timedelta(seconds=10))
    log_writer_by_minute.init_stream()
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
