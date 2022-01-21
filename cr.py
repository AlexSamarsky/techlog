from LogDataclasses import TimePatterns
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
init = False

def main():
    # /Users/samarskiy_a_s/projects/python/job/techlog/logs_new/rozn_1022/logz_ALL
    log_reader = LogReaderBaseVector('ppo2', 'logs_new/rozn_1022/logz_ALL')
    log_reader.set_time(datetime(2021, 10, 21, 18, 25, 0, 0), datetime(2021, 10, 21, 18, 45,  0, 0))
    
    log_filter = LogFilterPattern('ssid363', ',SessionID=55369')
    
    log_writer_connectid = LogWriteToCatalogByField('write_event', 'logs_new/rozn_1022/sessionid55369')
    # log_writer_connectid.add_data = False
    # log_writer_connectid.append_to_file = False
    # log_writer_connectid.concat_time = False
    # log_writer_connectid.field_as_file = True
    log_writer_connectid.field_name = 'SessionID'
    
    log_reader.connect(log_filter)
    log_filter.connect(log_writer_connectid)    

    log_writer_connectid.init_stream()
    
    log_reader.main()
    
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

