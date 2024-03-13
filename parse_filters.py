from bs4 import BeautifulSoup
from urllib.request import urlopen

import re
import sys
import datetime
import mysql.connector


filters = [
    { 'db_name': 'cities', 'name': 'gorodRegbook', 'columns': [ 'identifier', 'name', 'name_eng', 'country_ru' ] },
    { 'db_name': 'countries', 'name': 'countryId', 'columns': [ 'identifier', 'name', 'name_eng' ] },
    { 'db_name': 'types', 'name': 'statgr', 'columns': [ 'identifier', 'code', 'name', 'name_eng' ] },
    { 'db_name': 'classes', 'name': 'icecat', 'columns': [ 'identifier', 'name' ] }
]

def init_schema():
    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root'
    )

    cursor = db.cursor()
    cursor.execute('DROP DATABASE IF EXISTS regbook')
    cursor.execute('CREATE DATABASE IF NOT EXISTS regbook')

    with open('migrations/init_schema.sql', encoding='utf8') as source:
        cursor.execute(source.read(), multi=True)

    cursor.close()
    db.close()

def insert_into_filter(filter, values):
    # время по нулевому поясу
    # SET time_zone = '+00:00'
    ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'regbook'
    )

    cursor = db.cursor(prepared=True)

    filter_name = filter.get('db_name')
    columns = filter.get('columns').copy()
    params = list(values.values())

    cursor.execute(
        'SELECT id FROM `filter_{0}` WHERE identifier = %s'.format(filter_name),
        (values.get('identifier'),)
    )

    results = cursor.fetchall()
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
    db.commit()

    cursor.close()
    db.close()

    return

def parse_filter(filter):
    url = 'https://lk.rs-class.org/regbook/getDictionary2?d={filter_name}&f=formfield'.format(filter_name=filter['name'])
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')

    tr_index = 0
    trs = soup.find_all('tr')
    for tr in trs:
        tds = tr.find_all('td')
        values = {}
        td_index = 0
        for td in tds:
            a = td.find('a')
            a_onclick = a.get('onclick')
            matches = re.findall(r'[\,\']?([0-9A-Za-zА-Яа-я\-\s]+)[\,\']?', a_onclick, flags=re.U)
            if td_index == 0:
                values['identifier'] = matches[3]
            if filter['columns'][td_index+1] not in values.keys():
                values[filter['columns'][td_index+1]] = a.text
            td_index += 1
        if len(values.values()) > 0:
            insert_into_filter(filter, values)
        tr_index += 1

    return

# ----------------------------------------------------------------------------------------------------------------------

print('Время старта: ', datetime.datetime.now())

if sys.argv[1] == '1':
    print('---> Создать схему БД')
    init_schema()

if sys.argv[2] == '1':
    print('---> Спарсить фильтры')
    for filter in filters:
        parse_filter(filter)

print('Время окончания: ', datetime.datetime.now())
