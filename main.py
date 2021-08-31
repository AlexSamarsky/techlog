from LogEventAnalyze import LogEventAnalyze
from LogReaderVector import LogReaderBaseVector
from datetime import datetime, timedelta

from LogWrite import LogWriteToCatalogByField, LogWriteToConsole, LogWriteToFile
from LogFilter import LogFilterByEventName, LogFilterDuration, LogFilterPattern
from LogReader import LogReaderBase, LogReaderStream

profile = True

def main():

    # log_reader = LogReaderBaseVector('upp', '//office-sql/logz_all/all/rphost_8940/21082315.log')
    # # log_reader = LogReaderBaseVector('upp', '//office-sql/logz_all/all')
    # log_reader.set_time(datetime(2021, 8, 23, 15, 35, 0, 0), datetime(2021, 8, 23, 15, 45,  0, 0))
    
    # log_writer = LogWriteToCatalogByField('upp_write', 'logs_test/upp/test/all')
    # log_writer.field_name = 'SessionID'
    # log_writer.by_minute = True
    # # log_filter = LogFilterPattern('ssid13505', ',SessionID=13505')
    
    # log_reader.connect(log_writer)
    # # log_filter.connect(log_writer)
    # log_writer.init_stream()
    # log_reader.main()


    # # log_reader = LogReaderBaseVector('upp', '//office-sql/logz_all/all/rphost_8940/21082315.log')
    # log_reader = LogReaderBaseVector('upp', '//office-sql/logz_all/all')
    # log_writer = LogWriteToCatalogByField('upp_write', 'logs_test/upp/all', 'SessionID')
    # # log_filter = LogFilterPattern('ssid13505', ',SessionID=13505')
    # log_reader.connect(log_writer)
    # # log_filter.connect(log_writer)
    # log_reader.set_time(datetime(2021, 8, 23, 15, 35, 0, 0), datetime(2021, 8, 23, 15, 45,  0, 0))
    # log_writer.init_stream()
    # log_reader.main()



    # log_reader = LogReaderStream('bill', '//app-bill-nord/logz_full/Logs_full')
    # log_reader = LogReaderBase('test', '//onyx-1c-ppo2/Logz/PPO_Store_FULL')
    # log_reader = LogReaderStream('test', 'logs', 'settings.json')
    # log_reader = LogReaderStream('bill', 'logs/Logs_full/rphost_4180/', 'settings.json')
    # log_reader = LogReaderBase('bill', 'logs/Logs_full/rphost_4180/')
    
    # \\onyx-1c-ppo2\Logz\PPO_Store_FULL
    # f.raw_data = False
    # log_reader.set_time(datetime(2021, 8, 23, 11, 0, 0, 0), datetime(2021, 8, 23, 11, 0,  5, 437003))

    # log_writer_file = LogWriteToFile('write_file', 'logs_test/bill/test_out.log')
    # log_writer_console = LogWriteToConsole('console')
    # log_writer_bill = LogWriteToCatalogByMinute('write_by_minute', 'logs_test/bill/7_20')
    # log_filter = LogFilterPattern('callid', ',CallID=3408160')

    # log_reader.connect(log_writer_console)
    # log_reader.connect(log_writer_console)
    # log_reader.connect(log_filter)
    # log_filter.connect(log_writer_console)
    
    # Загрузка из биллинга
    # log_reader = LogReaderStream('bill', '//app-bill-nord/logz_full/Logs_full', 'settings.json')
    log_reader = LogReaderBaseVector('bill_test', 'logs_test/bill/main')
    log_reader.set_time(datetime(2021, 8, 27, 14, 0, 0, 0), datetime(2021, 8, 27, 14, 1,  0, 0))

    web_arr = ['VRSREQUEST', 'VRSRESPONSE']
    log_filter = LogFilterByEventName('web', web_arr)

    log_writer_by_minute = LogWriteToCatalogByField('write_by_minute', 'logs_test/bill/by_minute')
    log_writer_by_minute.add_data = False
    log_writer_by_minute.append_to_file = True
    # log_writer_by_minute.field_as_file = True
    log_writer_by_minute.field_name = 't:connectID'


    log_writer_events = LogWriteToCatalogByField('write_event', 'logs_test/bill/events')
    log_writer_events.add_data = False
    log_writer_events.append_to_file = False
    log_writer_events.field_as_file = True
    log_writer_events.concat_time = True
    log_writer_events.field_name = 't:connectID'

    # log_writer_by_minute.field_name = 'SessionID'
    # log_writer_by_minute.by_minute = True
    log_reader.connect(log_writer_by_minute)

    log_reader.connect(log_filter)
    log_filter.connect(log_writer_events)

    # log_reader.init_stream(datetime.now() - timedelta(seconds=10))
    log_writer_by_minute.init_stream()
    log_writer_by_minute.init_stream()
    log_reader.main()

    # все что связано с подсчетом длительности операций
    # log_reader_events = LogReaderStream('web_events', 'logs_test/bill/events', 'logs_test/bill/s_events.json')
    log_reader_events = LogReaderBaseVector('web_events', 'logs_test/bill/events/t_connectID=30830.log')
    log_reader_events.raw_data = False
    log_event_analyze = LogEventAnalyze('analyze', 'VRSREQUEST', 'VRSRESPONSE')

    log_filter_duration = LogFilterDuration('more_sec', 1_000_00)

    log_write_event = LogWriteToFile('write_to_file', 'logs_test/bill/t.log')

    log_reader_events.connect(log_event_analyze)
    log_event_analyze.connect(log_filter_duration)
    log_filter_duration.connect(log_write_event)
    log_reader_events.main()

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
