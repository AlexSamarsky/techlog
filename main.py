import os

from LogTaskCreator import TechLogTaskProcess
from LogExtractFields import TechLogExtractFields
from LogEventAnalyze import LogEventAnalyze
from LogReaderVector import LogReaderBaseVector
from datetime import datetime, timedelta

from LogWrite import LogWriteToCatalogByField, LogWriteToConsole, LogWriteToFile
from LogFilter import LogFilterByEventName, LogFilterByField, LogFilterDuration, LogFilterPattern
from LogReader import LogReaderStream

profile = True

def main():

    path_main = 'logs_test/bill2'
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
    # log_reader = LogReaderBaseVector('bill_test', 'logs_test/bill/main')
    # log_reader = LogReaderStream('bill_test', 'logs_test/bill/main', 'logs_test/bill/bill_main.json')
    # log_reader.set_time(datetime(2021, 8, 27, 14, 0, 0, 0), datetime(2021, 8, 27, 14, 5,  0, 0))

    log_reader_main = LogReaderStream('bill_test', '//app-bill-nord/logz_full/Logs_full', os.path.join(path_main, 'main.json'))
    # date1 = datetime(2021, 9, 3, 11, 0, 0, 0)
    # date2 = date1 + timedelta(minutes=5)
    # log_reader_main.set_time(date1, date2)
    # log_reader._multithreading = True

    log_filter_web = LogFilterByField('web_app', 't:applicationName', 'WebServerExtension')
    
    web_arr = ['VRSREQUEST', 'VRSRESPONSE']
    log_filter_respreq = LogFilterByEventName('web', web_arr)

    log_writer_by_minute = LogWriteToCatalogByField('write_by_minute', os.path.join(path_main, 'by_minute'))
    log_writer_by_minute.add_data = False
    log_writer_by_minute.append_to_file = True
    log_writer_by_minute.by_minute = True
    # log_writer_by_minute.field_as_file = True
    log_writer_by_minute.field_name = 't:connectID'


    log_writer_connectid = LogWriteToCatalogByField('write_event', os.path.join(path_main, 'events'))
    log_writer_connectid.add_data = False
    log_writer_connectid.append_to_file = True
    log_writer_connectid.field_as_file = True
    log_writer_connectid.concat_time = True
    log_writer_connectid.field_name = 't:connectID'

    log_reader_main.connect(log_filter_web)
    log_filter_web.connect(log_writer_by_minute)
    
    log_reader_main.connect(log_filter_respreq)
    log_filter_respreq.connect(log_writer_connectid)

    # все что связано с подсчетом длительности операций
    log_reader_events = LogReaderStream('web_events', os.path.join(path_main, 'events'), os.path.join(path_main, 'req_events.json'))
    # log_reader_events = LogReaderBaseVector('web_events', 'logs_test/bill/events')
    log_reader_events.raw_data = False
    # log_reader_events.read_count_limit = 1000
    
    log_event_analyze = LogEventAnalyze('analyze', 'VRSREQUEST', 'VRSRESPONSE',  os.path.join(path_main, 'by_minute'))

    log_filter_duration = LogFilterDuration('more_than_sec', 1_000_000)

    log_write_event = LogWriteToFile('write_to_file', os.path.join(path_main, 'events.log'))
    log_write_event.add_data = True
    log_write_event.append_to_file = True
    
    log_reader_events.connect(log_event_analyze)
    log_event_analyze.connect(log_filter_duration)
    log_filter_duration.connect(log_write_event)
    # print(log_reader_events.get_str())
    
    
    
    log_reader_tasks = LogReaderStream('tasks', os.path.join(path_main, 'events.log'),  os.path.join(path_main, 'tasks_settings.json'))
    log_reader_tasks.raw_data = False
    # log_reader_tasks.read_count_limit = 1
    
    fields = ['tla:start_time', 'tla:end_time', 'tla:path', 'tla:rel_path', 'tla:uuid']
    
    log_extract = TechLogExtractFields('extract', fields)
    log_reader_tasks.connect(log_extract)
    
    task_process = TechLogTaskProcess('task_p', os.path.join(path_main, 'detail'), 'tla:uuid', 'tla:start_time', 'tla:end_time', 'tla:path', 'tla:rel_path')
    log_extract.connect(task_process)
    
    # log_writer_by_minute.init_stream()
    # log_writer_connectid.init_stream()
    # log_write_event.init_stream()
    # log_reader_main.init_stream(datetime.now() - timedelta(seconds=10))
    
    # task_process.init_stream()
    # log_reader_events.init_stream()
    # log_reader_tasks.init_stream()
    # log_reader_main.init_stream()
    
    log_reader_main.set_time(end_time=datetime.now() - timedelta(seconds=1))
    
    log_reader_main.main()

    log_reader_events.main()

    log_reader_tasks.main()
    
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
