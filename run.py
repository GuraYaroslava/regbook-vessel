from urllib.request import urlopen
import re
from bs4 import BeautifulSoup
import mysql.connector

filters = [
    { 'db_name': 'cities', 'name': 'gorodRegbook', 'columns': [ 'name', 'name_eng', 'country_ru' ] },
    { 'db_name': 'countries', 'name': 'countryId', 'columns': [ 'name', 'name_eng' ] },
    { 'db_name': 'types', 'name': 'statgr', 'columns': [ 'code', 'name', 'name_eng' ] },
    { 'db_name': 'classes', 'name': 'icecat', 'columns': [ 'name' ] }
]

def init_schema():
    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root'
    )

    cursor = db.cursor()
    cursor.execute('CREATE DATABASE IF NOT EXISTS regbook')

    with open('migrations/init_schema.sql', encoding='utf8') as source:
        cursor.execute(source.read(), multi=True)

    cursor.close()
    db.close()

def insert_into_filter(filter, values):
    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'regbook'
    )

    cursor = db.cursor(prepared=True)

    filter_name = filter.get('db_name')
    filter_columns = ', '.join(filter.get('columns'))
    filter_values = ', '.join(map(lambda column: '%s', filter.get('columns')))
    sql = 'INSERT INTO `filter_{0}` ({1}) VALUES ({2})'.format(filter_name, filter_columns, filter_values)

    print(sql, list(values.values()))
    cursor.execute(sql, list(values.values()))

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

        insert_into_filter(filter, values)

        tr_index += 1

    return

# ----------------------------------------------------------------------------------------------------------------------

init_schema()

# a = {'db_name': 'classes',  'name': 'icecat', 'columns': [ 'identifier', 'name' ] }
# b = { 'identifier': 'Gura', 'name': 'Yaroslava' }
# b['name'] = 'Alex'
# insert_into_filter(a, b)

parse_filter({'db_name': 'countries', 'name': 'countryId', 'columns': ['identifier', 'name', 'name_eng' ] })
# parse_filter({'db_name': 'cities', 'name': 'gorodRegbook', 'columns': ['identifier', 'name', 'name_eng', 'country_ru' ] })
# parse_filter({'db_name': 'types', 'name': 'statgr', 'columns': ['identifier', 'code', 'name', 'name_eng' ] })
# parse_filter({'db_name': 'classes', 'name': 'icecat', 'columns': ['identifier', 'name' ] })

# for filter in filters:
#     parse_filter(filter)
