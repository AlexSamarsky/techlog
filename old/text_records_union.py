import sys
import os
import re
import time


def main(argv=None):
    start_time = time.time()

    # Получение списка параметров.
    if argv is None:
        argv = sys.argv

    # argv[0] - текущий исполняемый файл
    # argv[1] - исходная директория (путь)
    # argv[2] - выходной файл (txt)
    # argv[3] - время с
    # argv[4] - время по

    if len(argv) < 3:
        return

    if len(argv) > 3 and argv[3] != "":
        start_check_time = int(argv[3])
    else:
        start_check_time = 0

    if len(argv) > 4 and argv[4] != "":
        finish_check_time = int(argv[4])
    else:
        finish_check_time = 24

    pattern_start_record = re.compile(r"^\d{2}:\d{2}.\d{6}-\d+,")
    pattern_start_parameter_any = re.compile(r"^p_\d+: ")
    pattern_start_parameter = re.compile(r"^p_\d+: (?!'')")

    with open(argv[2], "w", encoding="utf-8-sig", errors='replace') as output_text:
        for item in os.walk(argv[1], topdown=False):
            for file_name in item[2]:
                file_path = os.path.join(item[0], file_name)
                log_hour = time.localtime(os.path.getmtime(file_path)).tm_hour
                if log_hour < start_check_time or log_hour >= finish_check_time:
                    continue
                with open(file_path, "r", encoding="utf-8-sig", errors='replace') as input_text:
                    line_list = input_text.readlines()

                if len(line_list) > 0:
                    prm = False
                    for n in range(len(line_list) - 1):
                        new_line = line_list[n]
                        if prm:
                            next_prm = pattern_start_parameter_any.match(line_list[n + 1])
                            if next_prm is None and not line_list[n + 1].isspace():
                                cur_prm = pattern_start_parameter.match(new_line)
                                if cur_prm:
                                    pass
                                if new_line.endswith("''\n") or cur_prm:
                                    prm = False
                            continue

                        new_line = new_line.replace('\\n', '\\\\n')
                        result = pattern_start_record.match(line_list[n + 1])
                        if result is None:
                            new_line = new_line.replace('\n', '\\n')

                        prm_index = new_line.find(",Prm=")

                        if prm_index != -1:
                            prm = True
                            new_line = new_line[:prm_index + 6]

                        output_text.write(new_line)

                    output_text.write(line_list[-1])

    finish_time = time.time()
    print(finish_time - start_time)


if __name__ == "__main__":
    sys.exit(main())

