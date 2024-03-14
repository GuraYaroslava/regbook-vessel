from bs4 import BeautifulSoup
from urllib.request import urlopen

import re
import sys
import datetime
import mysql.connector

class Filter:
    def __init__(self, ru_name, cite_name, db_name, db_columns):
        self.ru_name = ru_name
        self.cite_name = cite_name
        self.db_name = db_name
        self.db_columns = db_columns

# ----------------------------------------------------------------------------------------------------------------------

def init_schema():
    connection = mysql.connector.connect(host='localhost', user='root', password='root')
    cursor = connection.cursor()

    try:
        cursor.execute('DROP DATABASE IF EXISTS regbook')
        cursor.execute('CREATE DATABASE IF NOT EXISTS regbook')

        with open('migrations/init_schema.sql', encoding='utf8') as source:
            cursor.execute(source.read(), multi=True)

    finally:
        cursor.close()
        connection.close()

# ----------------------------------------------------------------------------------------------------------------------

def add_filter(filter, values):
    connection = mysql.connector.connect(host='localhost', user='root', password='root', database='regbook')
    cursor = connection.cursor(prepared=True)

    try:
        filter_name = filter.db_name

        sql = 'SELECT id FROM `filter_{0}` WHERE identifier = %s'.format(filter_name)
        params = (values.get('identifier'),)
        cursor.execute(sql, params)
        results = cursor.fetchall()

        # время по нулевому поясу
        # SET time_zone = '+00:00'
        ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        columns = filter.db_columns.copy()
        params = list(values.values())

        if cursor.rowcount == 0:
            columns.append('created_at')
            columns.append('updated_at')
            params.append(ts)
            params.append(ts)

            filter_columns = ', '.join(columns)
            filter_values = ', '.join(map(lambda column: '%s', columns))
            sql = 'INSERT INTO `filter_{0}` ({1}) VALUES ({2})'.format(filter_name, filter_columns, filter_values)

        else:
            columns.append('updated_at')
            params.append(ts)
            filter_values = ', '.join(map(lambda column: '{0} = %s'.format(column), columns))
            params.append(values.get('identifier'))
            sql = 'UPDATE filter_{0} SET {1} WHERE filter_{0}.identifier = %s'.format(filter_name, filter_values)

        cursor.execute(sql, params)
        connection.commit()

    finally:
        cursor.close()
        connection.close()

    return

# ----------------------------------------------------------------------------------------------------------------------

def parse_filter(filter):
    url = 'https://lk.rs-class.org/regbook/getDictionary2?d={0}&f=formfield'.format(filter.cite_name)
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')

    tr_index = 0; trs = soup.find_all('tr')
    for tr in trs:
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
            add_filter(filter, values)

        tr_index += 1

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

if sys.argv[1] == '1':
    schema_start_time = datetime.datetime.now()
    print_start_status('Создать схему БД')
    init_schema()
    print_end_status(schema_start_time)

if sys.argv[2] == '1':
    filter_start_time = datetime.datetime.now()
    print_start_status('Спарсить фильтры', 2)
    filters = [
        Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ]),
        Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ]),
        Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ]),
        Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ])
    ]
    for filter in filters:
        start_time = datetime.datetime.now()
        print_start_status('фильтр '+filter.ru_name.upper(), 3)
        parse_filter(filter)
        print_end_status(start_time, 3)
    print_end_status(filter_start_time, 2)

print_end_status(total_start_time, 0)
