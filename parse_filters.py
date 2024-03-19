from bs4 import BeautifulSoup
from urllib.request import urlopen
from decouple import config
from threading import Thread
import re
import sys
import datetime
import mysql.connector
import multiprocessing


# ----------------------------------------------------------------------------------------------------------------------

def get_db_connection(with_db=True):
    host = config('DB_HOST')
    user = config('DB_USERNAME')
    password = config('DB_PASSWORD')
    database = config('DB_DATABASE') if with_db else ''

    return mysql.connector.connect(host=host, user=user, password=password, database=database)

# ----------------------------------------------------------------------------------------------------------------------

# Удалить существующую БД, если есть и создать новую со всеми таблицами
def init_schema():
    database = config('DB_DATABASE')
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        cursor.execute('DROP DATABASE IF EXISTS {}'.format(database))
        cursor.execute('CREATE DATABASE IF NOT EXISTS {}'.format(database))

        with open('migrations/init_schema.sql', encoding='utf8') as source:
            for result in cursor.execute(source.read(), multi=True):
                print(result.statement)

    except mysql.connector.Error as error:
        print(error)

    finally:
        cursor.close()
        connection.close()

# ----------------------------------------------------------------------------------------------------------------------

class Filter(Thread):
    # @param args { ru_name, cite_name, db_name, db_columns }
    def __init__(self, args):
        # Thread
        Thread.__init__(self)

        # человеческое название
        self.ru_name = args['ru_name']
        # название фильтра для составления запроса
        self.cite_name = args['cite_name']
        # название таблицы в БД
        self.db_name = args['db_name']
        # список нзваний колонок в таблице БД
        self.db_columns = args['db_columns']

        self.start_parsing_time = datetime.datetime.now()
        self.end_parsing_time = datetime.datetime.now()

    # Добавить записть в таблицу данного фильтра
    # @param dict values
    def add_record(self, values):
        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            sql = 'SELECT id FROM `filter_{0}` WHERE identifier = %s'.format(self.db_name)
            params = (values.get('identifier'),)
            cursor.execute(sql, params)
            results = cursor.fetchall()

            # время по нулевому поясу
            # SET time_zone = '+00:00'
            ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            columns = list(values.keys())
            params = list(values.values())

            if cursor.rowcount == 0:
                columns.append('created_at')
                columns.append('updated_at')
                params.append(ts)
                params.append(ts)

                filter_columns = ', '.join(columns)
                filter_values = ', '.join(map(lambda column: '%s', columns))
                sql = 'INSERT INTO `filter_{0}` ({1}) VALUES ({2})'.format(self.db_name, filter_columns, filter_values)

            else:
                columns.append('updated_at')
                params.append(ts)
                filter_values = ', '.join(map(lambda column: '{0} = %s'.format(column), columns))
                params.append(values.get('identifier'))
                sql = 'UPDATE filter_{0} SET {1} WHERE filter_{0}.identifier = %s'.format(self.db_name, filter_values)

            cursor.execute(sql, params)
            connection.commit()

        finally:
            cursor.close()
            connection.close()

        return

    # Спарсить все значения данного фильтра
    def parse(self):
        url = 'https://lk.rs-class.org/regbook/getDictionary2?d={0}&f=formfield'.format(self.cite_name)
        page = urlopen(url)
        html = page.read().decode('utf-8')
        soup = BeautifulSoup(html, 'html.parser')

        for tr in soup.find_all('tr'):
            values = {}; td_index = 0; tds = tr.find_all('td')
            for td in tds:
                a = td.find('a')
                a_onclick = a.get('onclick')
                matches = re.findall(r'[\,\']?([0-9A-Za-zА-Яа-я\-\s]+)[\,\']?', a_onclick, flags=re.U)

                if td_index == 0:
                    values['identifier'] = matches[3]

                if self.db_columns[td_index+1] not in values.keys():
                    values[self.db_columns[td_index+1]] = a.text

                td_index += 1

            if len(values.values()) > 0:
                self.add_record(values)

        return

    # Thread
    def run(self):
        self.start_parsing_time = datetime.datetime.now()
        self.parse()
        self.end_parsing_time = datetime.datetime.now()

    # Вермя работы потока
    def parse_duration(self):
        return (self.end_parsing_time - self.start_parsing_time).total_seconds()

# ----------------------------------------------------------------------------------------------------------------------

def print_start_status(caption, level=1, current_datetime=None):
    current_datetime = current_datetime if current_datetime is not None else datetime.datetime.now()
    separator = '---' * level + '>'
    print('[{}]'.format(current_datetime), separator, caption)
    return

def print_end_status(start_datetime, level=1, caption='Завершено'):
    end_datetime = datetime.datetime.now()
    duration = (end_datetime - start_datetime).total_seconds()
    separator = '---' * level + '>'
    print('[{}]'.format(datetime.datetime.now()), separator, caption+':', duration, '(s)')
    return

# ======================================================================================================================

init_filters = [
    { 'ru_name': 'Города', 'cite_name': 'gorodRegbook', 'db_name': 'cities', 'db_columns': [ 'identifier', 'name', 'name_eng', 'country_ru' ] },
    { 'ru_name': 'Страны', 'cite_name': 'countryId', 'db_name': 'countries', 'db_columns': [ 'identifier', 'name', 'name_eng' ] },
    { 'ru_name': 'Статистические группы судов', 'cite_name': 'statgr', 'db_name': 'types', 'db_columns': [ 'identifier', 'code', 'name', 'name_eng' ] },
    { 'ru_name': 'Ледовые категории', 'cite_name': 'icecat', 'db_name': 'classes', 'db_columns': [ 'identifier', 'name' ] }
]

filters = []; for args in init_filters: filters.append(Filter(args))

# ----------------------------------------------------------------------------------------------------------------------

total_start_time = datetime.datetime.now()
total_caption = 'Начало работы скрипта'
print_start_status(total_caption, 0)

# ----------------------------------------------------------------------------------------------------------------------

if len(sys.argv) >= 2 and sys.argv[1] == '1':
    schema_start_time = datetime.datetime.now(); caption = 'Создать схему БД'
    print_start_status(caption)
    init_schema()
    print_end_status(schema_start_time, 1, caption)

# ----------------------------------------------------------------------------------------------------------------------

if len(sys.argv) >= 3 and sys.argv[2] == '1':
    filter_start_time = datetime.datetime.now(); caption = 'Спарсить фильтры'
    print_start_status(caption)
    for filter in filters:
        start_time = datetime.datetime.now()
        print_start_status('Фильтр '+filter.ru_name.upper(), 2)
        filter.parse()
        print_end_status(start_time, 3)
    print_end_status(filter_start_time, 1, caption)

# ----------------------------------------------------------------------------------------------------------------------

if len(sys.argv) >= 3 and sys.argv[2] == '2':
    filter_start_time = datetime.datetime.now(); caption = 'Спарсить фильтры потоками'
    for filter in filters: filter.start()
    # Подождем, пока все потоки завершат свою работу
    for filter in filters: filter.join()
    for filter in filters: print_start_status('Фильтр '+filter.ru_name.upper()+f': {filter.parse_duration()} (s)', 2)
    print_end_status(filter_start_time, 1, caption)

# ----------------------------------------------------------------------------------------------------------------------

def sequential(filter_chunk, proc):
    for filter_args in filter_chunk:
        start_time = datetime.datetime.now()
        filter = Filter(filter_args); filter.parse()
        print_end_status(start_time, 2, f'[proc #{proc}] Фильтр '+filter.ru_name.upper())

if len(sys.argv) >= 3 and sys.argv[2] == '3':
    filter_start_time = datetime.datetime.now(); caption = 'Спарсить фильтры мультипроцессорно'
    print_start_status(caption)
    n_proc = multiprocessing.cpu_count()
    n_filter = len(init_filters)
    n = int(n_filter / n_proc) if n_filter % n_proc == 0 else int(n_filter // n_proc + 1)
    print(f'Кол-во ядер: {n_proc} |', f'Кол-во фильтров: {n_filter} |', f'Кол-во фильтров на ядро: {n}')
    init = []; index = 1
    for filter_chunk in [init_filters[i:i + n] for i in range(0, n_filter, n)]:
        init.append((filter_chunk, index)); index += 1
    with multiprocessing.Pool() as pool:
       pool.starmap(sequential, init)
    print_end_status(filter_start_time, 1, caption)

# ----------------------------------------------------------------------------------------------------------------------

print_end_status(total_start_time, 0, total_caption)
