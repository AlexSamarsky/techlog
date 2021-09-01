from LogReaderVector import LogReaderBaseVector
from datetime import datetime, timedelta

from LogWrite import LogWriteToCatalogByField, LogWriteToCatalogByMinute, LogWriteToConsole, LogWriteToFile
from LogFilter import LogFilterPattern
from LogReader import LogReaderStream

profile = False

def main():

    field_name = 't:connectID'

    # log_reader.set_time(datetime(2021, 8, 20, 11, 0, 0, 0), datetime(2021, 8, 20, 11, 3,  0, 0))

    # log_writer_bill = LogWriteToCatalogByMinute('write_by_minute', 'logs_test/bill/11_00')
    # log_reader.connect(log_writer_bill)

    # log_reader.main()


    log_reader_cache = LogReaderBaseVector('bill_cache', 'logs_test/bill/11_00')
    log_filter = LogFilterPattern('web', ',VRSRESPONSE,|,VRSREQUEST,')
    log_write_bill_web = LogWriteToCatalogByField('write_by_connectID', 'logs_test/bill/by_connectID', field_name)
    
    log_reader_cache.connect(log_filter)

    log_filter.connect(log_write_bill_web)

    log_reader_cache.main()

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
