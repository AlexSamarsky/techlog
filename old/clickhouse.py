import datetime
from clickhouse_driver import Client
import csv
import sys
from csv import DictReader
import pprint


class ClickHouseWrapper:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.client = self._init_client()
        self.converters = {
            '_duration': (int, 'Int64')
        }

    def _init_client(self):
        client = Client(host=self.host, port=self.port,
                        database=self.database, user=self.user, password=self.password)
        return client

    def create_table_from_csv(self, table_name, csv_path):
        with open(csv_path, 'r', encoding='utf-8', newline='') as file_handler:
            table_columns = ','.join(
                list(map(lambda x: f'{x.replace(":", "_")} {self.converters[x][1] if x in self.converters else "String"} \n', file_handler.readline().split(','))))
        create_query = f'CREATE TABLE {table_name} ({table_columns}) ENGINE = MergeTree() ORDER BY (_duration)'
        result = self.client.execute(create_query)
        return result

    def drop_table(self, table_name):
        result = self.client.execute(f'DROP TABLE IF EXISTS {table_name}')
        return result

    def get_tables_list(self):
        return self.client.execute('SHOW TABLES')

    def insert_csv(self, table_name, csv_path):
        result = self.client.execute(f'INSERT INTO {table_name} VALUES', self.__iter_csv(csv_path))
        return result

    def __iter_csv(self, filename):
        counter = 0
        with open(filename, 'r', encoding='utf-8-sig') as f:
            reader = DictReader(f)
            for line in reader:
                counter += 1
                if counter == 1:
                    continue
                yield {k: (self.converters[k][0](v) if k in self.converters else v) for k, v in line.items()}


class ClickHouseWrapperUtils:

    @staticmethod
    def set_field_size_limit():
        max_int = sys.maxsize
        while True:
            # decrease the maxInt value by factor 10
            # as long as the OverflowError occurs.
            try:
                csv.field_size_limit(max_int)
                break
            except OverflowError:
                max_int = int(max_int / 10)


def main(argv=None):
    # Получение списка параметров.
    if argv is None:
        argv = sys.argv

    params_str = """
    # Определение имен входных/выходных файлов
    # argv[1] host
    # argv[2] port
    # argv[3] database
    # argv[4] user
    # argv[5] password
    # argv[6] table name
    # argv[7] путь к csv
    """
    if len(argv) < 7:
        print(f'Недостаточно входных параметров. Описание параметров:{params_str}')
        return
    # Входные параметры.
    csv_name = argv[7]
    table_name = argv[6]

    # Принтер
    pp = pprint.PrettyPrinter(indent=4)

    # Установка максимального лимита размера колонки csv
    ClickHouseWrapperUtils.set_field_size_limit()

    print(datetime.datetime.now())
    print('Подключение к clickhouse...')
    wrapper = ClickHouseWrapper(*argv[1:6])

    print(datetime.datetime.now())
    print(f'Удаление таблицы {table_name}...')
    wrapper.drop_table(table_name)

    print(datetime.datetime.now())
    print(f'Создание таблицы {table_name}...')
    wrapper.create_table_from_csv(table_name, csv_name)

    print(datetime.datetime.now())
    print(f'Заполнение таблицы {table_name} из csv {csv_name}...')
    wrapper.insert_csv(table_name, csv_name)

    print(datetime.datetime.now())
    print('Список таблиц:')
    pp.pprint(wrapper.get_tables_list())


if __name__ == "__main__":
    sys.exit(main())
