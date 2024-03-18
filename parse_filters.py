from bs4 import BeautifulSoup
from urllib.request import urlopen
from decouple import config
import re
import sys
import datetime
import mysql.connector


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

class Filter:
    def __init__(self, ru_name, cite_name, db_name, db_columns):
        # человеческое название
        self.ru_name = ru_name
        # название фильтра для составления запроса
        self.cite_name = cite_name
        # название таблицы в БД
        self.db_name = db_name
        # список нзваний колонок в таблице БД
        self.db_columns = db_columns

    # Добавить записть в таблицу данного фильтра
    # @param dict values
    def add_row(self, values):
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

# ----------------------------------------------------------------------------------------------------------------------

# Спарсить все значения данного фильтра
# @param Filter filter
def parse_filter(filter):
    url = 'https://lk.rs-class.org/regbook/getDictionary2?d={0}&f=formfield'.format(filter.cite_name)
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

            if filter.db_columns[td_index+1] not in values.keys():
                values[filter.db_columns[td_index+1]] = a.text

            td_index += 1

        if len(values.values()) > 0:
            filter.add_row(values)

    return

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
    print('[{}]'.format(datetime.datetime.now()), separator, caption, duration, '(s)')
    return

# ----------------------------------------------------------------------------------------------------------------------

total_start_time = datetime.datetime.now()
print_start_status('Начало работы скрипта', 0)

if len(sys.argv) >= 2 and sys.argv[1] == '1':
    schema_start_time = datetime.datetime.now()
    print_start_status('Создать схему БД')
    init_schema()
    print_end_status(schema_start_time)

if len(sys.argv) >= 3 and sys.argv[2] == '1':
    filter_start_time = datetime.datetime.now()
    print_start_status('Спарсить фильтры')
    filters = [
        Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ]),
        Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ]),
        Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ]),
        Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ])
    ]
    for filter in filters:
        start_time = datetime.datetime.now()
        print_start_status('Фильтр '+filter.ru_name.upper(), 2)
        parse_filter(filter)
        print_end_status(start_time, 3)
    print_end_status(filter_start_time)

print_end_status(total_start_time, 0)
