from LogDataclasses import RePatterns
import codecs
# s = "�ВЖурнале(ДанныеОперации, ПокупательСсылка, ТекстОшибки);\n\t\tОбщийМодуль.ОбщегоНазначения.Модуль : 135 : Запись.Записать();'\n06:14.833035-2,CALL,1,process=rmngr,p:processName=RegMngrCntxt,p:processName=ServerJobExecutorContext,t:clientID=4546,"

file_name = 'logs\\Logs_full\\rmngr_3236\\21082312.log'

with open(file_name, 'rb') as f:
    
    # f_seek = 176580197
    f_seek = 176580303

    f.seek(f_seek)
    # f_seek += 3
    b_text = f.read(200)

    text = b_text.decode('utf-8', 'replace')

    # if bytes(text[0], 'utf-8') == b'\xef\xbf\xbd':
    #     text = text[1:]
    #     f_seek += 1

    # rs2 = rs2[1:]
    match = RePatterns.re_new_event.search(text[5:6])
    inc = 219

    # text = rs2.encode('utf8')

    # f.seek(f_seek + inc)
    # text_after = f.read(1000)

    text_before = text[:match.start()]
    b_text_before = bytes(text_before, 'utf-8')
    len_text_before = len(b_text_before)
    
    last_actual_seek_position = f_seek + len_text_before + 1

    f.seek(last_actual_seek_position)
    text2 = f.read(100)
    match_event = RePatterns.re_new_event.match(text2)
    print(text2[:20])

