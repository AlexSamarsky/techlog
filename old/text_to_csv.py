import sys
import re
import csv
import time


def main(argv=None):
    start_time = time.time()

    # Получение списка параметров.
    if argv is None:
        argv = sys.argv

    # argv[0] - текущий исполняемый файл
    # argv[1] - исходный файл(txt)
    # argv[2] - выходной файл (csv)
    # argv[3] - файл с именами свойств

    if len(argv) < 3:
        return

    with open(argv[3], "r", encoding="utf-8-sig", errors='replace') as input_name:
        prop_name_list = input_name.readline().split(',')

    pattern_prop = re.compile(",[\w\d][\w\d:]*=", flags=re.ASCII)
    pattern_prop_quotes = re.compile("',[\w\d][\w\d:]*=", flags=re.ASCII)
    pattern_prop_double = re.compile(r"\",[\w\d][\w\d:]*=", flags=re.ASCII)

    with open(argv[1], "r", encoding="utf-8-sig", errors='replace') as input_text:
        csv_list = input_text.readlines()

    csv_list_db = [['_time', '_duration', '_event', '_level']]

    for n in range(len(csv_list)):
        new_line = csv_list[n].strip("\n").replace('-',',',1)
        record_list = ["" for n in range(len(csv_list_db[0]))]
        record_list[:4] = new_line.split(',', 4)[:4]
        new_prop = pattern_prop.search(new_line)
        its_quote = 0
        while new_prop:
            name = new_prop.group()[1+its_quote:-1].lower()
            qs = new_prop.end()
            if name in prop_name_list:
                name = name.replace(":", "_")
                if qs < len(new_line):
                    pref = new_line[qs]
                    its_quote = int(pref == '\'' or pref == '\"')
                    if pref == '\'':
                        new_prop = pattern_prop_quotes.search(new_line, qs)
                    elif pref == '\"':
                        new_prop = pattern_prop_double.search(new_line, qs)
                    else:
                        new_prop = pattern_prop.search(new_line, qs)
                    if new_prop:
                        qe = new_prop.start()
                        value = new_line[qs:qe+its_quote]
                    else:
                        value = new_line[qs:]
                else:
                    value = ""
                    new_prop = None
            else:
                new_prop = pattern_prop.search(new_line, qs)
                continue
            if name in csv_list_db[0]:
                index = csv_list_db[0].index(name)
                record_list[index] = value
            else:
                csv_list_db[0].append(name)
                record_list.append(value)

        csv_list_db.append(record_list)

    with open(argv[2], "w", encoding="utf-8-sig", errors='replace') as csv_file_db:
        csv_writer_db = csv.writer(csv_file_db, delimiter=',', lineterminator='\n')
        for n in range(len(csv_list)):
            csv_writer_db.writerow(csv_list_db[n])
        csv_writer_db.writerow(csv_list_db[-1])

    finish_time = time.time()
    print(finish_time - start_time)

if __name__ == "__main__":
    sys.exit(main())
