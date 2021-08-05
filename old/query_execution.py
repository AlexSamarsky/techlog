import sys
import pandas as pd

def main(argv=None):
    # Получение списка параметров.
    if argv is None:
        argv = sys.argv

    # argv[0] - текущий исполняемый файл
    # argv[1] - исходный файл(csv)
    # argv[2] - выходной файл (csv)
    # argv[3] - текст запроса
    # argv[4] - словарь параметров

    if len(argv) < 3:
        return

    query_text = argv[3].replace("t1", "data_tj")
    query_text = query_text.replace("t_res", "data_event")

    data_tj = pd.read_csv(argv[1], low_memory=False)

    #data_event = data_tj[data_tj._event.isin(event_list)].sort_values(property_main, ascending=False).drop_duplicates().head(N)
    print(query_text)

    data_event = eval(query_text)

    print(argv[2])
    data_event.to_csv(argv[2], index=False)



if __name__ == "__main__":
    sys.exit(main())