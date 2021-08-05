import sys
import re
import csv
import time
# from tech_log_analysis_utils import TechLogAnalysisUtils


def main(argv=None):

    # Установка максимального лимита размера колонки csv
    # TechLogAnalysisUtils.set_field_size_limit()

    start_time = time.time()

    # Получение списка параметров.
    if argv is None:
        argv = sys.argv

    # argv[0] - текущий исполняемый файл
    # argv[1] - исходный файл(csv)
    # argv[2] - выходной файл (csv)

    if len(argv) < 3:
        return

    pattern_table = re.compile(r"tt\d\d*", flags=re.ASCII)
    pattern_table1 = re.compile(r"\w+-\w+-\w+-\w+-\w+", flags=re.ASCII)
    pattern_table2 = re.compile(r"\b_Q_\d{3}_F_\d{3}", flags=re.ASCII)
    pattern_table3 = re.compile(r"T\d+", flags=re.ASCII)
    pattern_table4 = re.compile(r"p_0.*", flags=re.ASCII)
    pattern_table5 = re.compile(r"\(\d+\)", flags=re.ASCII)

    with open(argv[1], "r", encoding="utf-8-sig", errors='replace') as input_csv, open(argv[2], "w", encoding="utf-8-sig", errors='replace') as output_csv:
        csv_dict_reader = csv.DictReader(input_csv, delimiter=',')
        csv_dict_writer = csv.DictWriter(output_csv, delimiter=',', fieldnames=csv_dict_reader.fieldnames, lineterminator='\n')
        csv_dict_writer.writeheader()
        for row in csv_dict_reader:
            if row.get('sql'):
                query_text = row['sql']
                table = pattern_table.search(query_text)
                while table:
                    query_text = query_text[:table.start()+ 2] + query_text[table.end():]
                    table = pattern_table.search(query_text)
                table = pattern_table1.search(query_text)
                while table:
                    query_text = query_text[:table.start()] + "GUID" + query_text[table.end():]
                    table = pattern_table1.search(query_text)
                table = pattern_table2.search(query_text)
                while table:
                    query_text = query_text[:table.start()] + "QField" + query_text[table.end():]
                    table = pattern_table2.search(query_text)
                table = pattern_table3.search(query_text)
                while table:
                    query_text = query_text[:table.start()] + "T" + query_text[table.end():]
                    table = pattern_table3.search(query_text)
                table = pattern_table4.search(query_text)
                while table:
                    query_text = query_text[:table.start()]
                    table = pattern_table4.search(query_text)
                table = pattern_table5.search(query_text)
                while table:
                    query_text = query_text[:table.start()] + "({NUM})" + query_text[table.end():]
                    table = pattern_table5.search(query_text)

                row['sql'] = query_text
            csv_dict_writer.writerow(row)

    finish_time = time.time()
    print(finish_time - start_time)

if __name__ == "__main__":
    sys.exit(main())

