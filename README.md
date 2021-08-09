# tech_log_analysis 

Инструменты анализа технологического журнала 1С с возможностью произвольных запросов

TechLogFile - класс по склеиванию кучи логов в один файл
    file_in_name - входящий файл или каталог
    file_out_name - файл в котором будет общий лог

    filter_line - можно переопределить процедуру и фильтровать строки. возвратить пустую строку для пропуска
        line

TechLogCsv - класс по преобразованию логов в csv файл
    file_in_name - входящий файл или каталог
    file_out_name - файл в котором будет общий лог
    file_field_filter_name - фильтр наименований событий

    field_value_process - обработка значения поля и можно его преобразовать
        field_name, field_value

TechLogCsvNormalization
    file_in_name - входящий файл или каталог
    file_out_name - файл в котором будет общий лог
    file_field_filter_name - фильтр наименований событий

    переопределяем filter_line, field_value_process