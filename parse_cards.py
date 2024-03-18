from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen
import json

import re
import sys
import csv
import datetime
import mysql.connector


# ----------------------------------------------------------------------------------------------------------------------

def get_db_connection():
    host = config('DB_HOST')
    user = config('DB_USERNAME')
    password = config('DB_PASSWORD')
    database = config('DB_DATABASE')

    return mysql.connector.connect(host=host, user=user, password=password, database=database)

# ----------------------------------------------------------------------------------------------------------------------

class Card:
    def __init__(self, identifier):
        self.identifier = identifier

    # Получить характеристики из БД
    def get_data(self):
        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            sql = """
                select
                  group_properties.name as `group`,
                  properties.name as `property`,
                  cards_properties.property_value as `value`
                from
                  cards_properties
                  left join cards on cards.id = cards_properties.card_id
                  left join properties on properties.id = cards_properties.property_id
                  left join group_properties on group_properties.id = properties.group_id
                where
                  cards.identifier = %s
                order by group_properties.id, properties.id;
            """

            cursor.execute(sql, (self.identifier,))
            result = cursor.fetchall()

        finally:
            cursor.close()
            connection.close()

        return result

    # Сравнить значения характеристик из БД с сайтом
    def cmp_with_cite(self):
        db_card = self.get_data()

        url = 'https://lk.rs-class.org/regbook/vessel?fleet_id={0}&a=print'.format(self.identifier)
        page = urlopen(url)
        html = page.read().decode('utf-8')
        soup = BeautifulSoup(html, 'html.parser')

        group_name = ''; city_card = list()
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td'); td_index = 0; property_id = None; property_name = ''; property_value = ''
            for td in tds:
                if td.get('colspan') is not None:
                    group_name = td.find('h3').text.strip()
                else:
                    if td_index == 0:
                        property_name = td.text.strip()
                    elif td_index == 1:
                        property_value = td.get_text(separator='\n').strip()
                td_index += 1

            if property_value != '':
                city_card.append((group_name, property_name, property_value))

        success = True
        for city_property in city_card:
            is_exist = False

            for db_property in db_card:
                equality = True; column_index = 0
                for db_value in db_property:
                    equality = equality and db_value.lower() == city_property[column_index].lower()
                    column_index += 1
                is_exist = is_exist or equality

            if is_exist == False:
                print_start_status('Карточка #'+identifier+': совпадение не найдено', 3)

            success = success and is_exist

        print_start_status(('успех' if success == True else 'провал'), 3)

        return

    # Выгрузить характеристики из БД в файл формата .csv
    def export(self):
        csv_writer = csv.writer(open('cards/'+self.identifier+'.csv', 'w'))
        for row in self.get_data():
            csv_writer.writerow(row)

        return

# ----------------------------------------------------------------------------------------------------------------------

class Filter:
    def __init__(self, ru_name, cite_name, db_name, db_columns, db_relationship):
        self.ru_name = ru_name
        self.cite_name = cite_name
        self.db_name = db_name
        self.db_columns = db_columns
        self.db_relationship = db_relationship

    # Получить список значений фильтра
    def get_list(self):
        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            sql = """
                select `{0}` as name, `{1}` as field, identifier as value from filter_{2} order by id
            """.format(self.cite_name, self.db_relationship, self.db_name)
            cursor.execute(sql, ())
            result = cursor.fetchall()

        finally:
            cursor.close()
            connection.close()

        return result

# ----------------------------------------------------------------------------------------------------------------------

# Сохранить в БД, если группа характеристик новая, вернуть id группы характеристик
def get_or_create_group(name):
    group_id = None

    connection = get_db_connection()
    cursor = connection.cursor(prepared=True)

    try:
        cursor.execute('SELECT id FROM group_properties WHERE name = %s', (name,))
        results = cursor.fetchall()

        if cursor.rowcount == 0:
            cursor.execute('INSERT INTO group_properties (name) VALUES (%s)', (name,))
            group_id = cursor.lastrowid
            connection.commit()
        else:
            group_id = results[0][0]

    finally:
        cursor.close()
        connection.close()

    return group_id

# ----------------------------------------------------------------------------------------------------------------------

# Сохранить в БД, если характеристика новая, вернуть id характеристики
def get_or_create_property(name, group_id):
    property_id = None

    connection = get_db_connection()
    cursor = connection.cursor(prepared=True)

    try:
        cursor.execute('SELECT id FROM properties WHERE name = %s AND group_id = %s', (name, group_id))
        results = cursor.fetchall()

        if cursor.rowcount == 0:
            cursor.execute('INSERT INTO properties (name, group_id) VALUES (%s, %s)', (name, group_id))
            property_id = cursor.lastrowid
            connection.commit()
        else:
            property_id = results[0][0]

    finally:
        cursor.close()
        connection.close()

    return property_id

# ----------------------------------------------------------------------------------------------------------------------

# Сохранить в БД, если карточка судна новая, вернуть id карточки судна
def get_or_create_card(identifier):
    card_id = None

    connection = get_db_connection()
    cursor = connection.cursor(prepared=True)

    try:
        cursor.execute('SELECT id FROM cards WHERE identifier = %s', (identifier,))
        results = cursor.fetchall()

        if cursor.rowcount == 0:
            cursor.execute('INSERT INTO cards (identifier) VALUES (%s)', (identifier,))
            card_id = cursor.lastrowid
            connection.commit()
        else:
            card_id = results[0][0]

    finally:
        cursor.close()
        connection.close()

    return card_id

# ----------------------------------------------------------------------------------------------------------------------

def create_or_update_card_properties(properties):
    for property in properties:
        create_or_update_card_property(property)

    return

# Сохранить в БД, что карточка имеет характеристику с данным значением
def create_or_update_card_property(property):
    card_id = property['card_id']
    property_id = property['property_id']
    property_value = property['property_value']
    ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    connection = get_db_connection()
    cursor = connection.cursor(prepared=True)

    try:
        sql = 'SELECT id FROM cards_properties WHERE card_id = %s AND property_id = %s'
        cursor.execute(sql, (card_id, property_id))
        result = cursor.fetchone()

        if result is None:
            sql = 'INSERT INTO cards_properties (card_id, property_id, property_value, updated_at) VALUES (%s, %s, %s, %s)'
            cursor.execute(sql, (card_id, property_id, property_value, ts))
        else:
            id = result[0]
            sql = 'UPDATE cards_properties SET property_value = %s, updated_at = %s WHERE id = %s'
            cursor.execute(sql, (property_value, ts, id))

        connection.commit()

    finally:
        cursor.close()
        connection.close()

    return

# ----------------------------------------------------------------------------------------------------------------------

# Сохранить в БД, что карточка ищется по данным фильтрам с соответсвующими значениями
# filters [ {
#   name,   # название фильтра для составление запроса
#   value,  # значение фильтра
#   field   # название поля под фильтр в таблице связей cards_filters
# }, ]
def create_or_update_card_filters(card_id, filters):
    connection = get_db_connection()
    cursor = connection.cursor(prepared=True)

    try:
        for filter in filters:
            cursor.execute('SELECT id FROM cards_filters WHERE card_id = %s', (card_id,))
            result = cursor.fetchone()

            if result is None:
                sql = 'INSERT INTO cards_filters (card_id, {0}) VALUES (%s, %s)'.format(filter['field'])
                cursor.execute(sql, (card_id, filter['value']))
            else:
                sql = 'UPDATE cards_filters SET {0} = %s WHERE card_id = %s'.format(filter['field'])
                cursor.execute(sql, (filter['value'], card_id))

        connection.commit()

    finally:
        cursor.close()
        connection.close()

    return

# ----------------------------------------------------------------------------------------------------------------------

def create_or_replace_card_certificates(card_id, certificates):
    return create_or_replace_card_(card_id, certificates, 'card_certificates')

def create_or_replace_card_contacts(card_id, contacts):
    return create_or_replace_card_(card_id, contacts, 'card_contacts')

def create_or_replace_card_states(card_id, states):
    return create_or_replace_card_(card_id, states, 'card_states')

def create_or_replace_card_(card_id, records, db_name):
    connection = get_db_connection()
    cursor = connection.cursor(prepared=True)

    try:
        cursor.execute('DELETE FROM {0} WHERE card_id = %s'.format(db_name), (card_id,))
        for record in records:
            columns = list(record.keys())
            query_columns = ', '.join(columns)
            query_values = ', '.join(map(lambda column: '%s', columns))
            params = list(record.values())
            sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(db_name, query_columns, query_values)
            cursor.execute(sql, params)

        connection.commit()

    finally:
        cursor.close()
        connection.close()

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

# Спарсить карточку с характеристиками судна
def parse_card_by_identifier(card_identifier, filters=None, with_status=False):
    card_id = get_or_create_card(card_identifier)

    url = 'https://lk.rs-class.org/regbook/vessel?fleet_id={0}&a=print'.format(card_identifier)
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')

    group_id = None; group_name = ''; card_properties = list()
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td'); td_index = 0; property_id = None; property_name = ''; property_value = ''
        for td in tds:
            if td.get('colspan') is not None:
                group_name = td.find('h3').text.strip()
            else:
                if td_index == 0:
                    property_name = td.text.strip()
                elif td_index == 1:
                    property_value = td.get_text(separator='\n').strip()
            td_index += 1

        if group_name != '':
            group_id = get_or_create_group(group_name)

        if property_name != '' and group_id is not None:
            property_id = get_or_create_property(property_name, group_id)

        if property_id is not None and property_value != '':
            card_properties.append({ 'card_id': card_id, 'property_id': property_id, 'property_value': property_value })

    create_or_update_card_properties(card_properties)

    if filters is not None and card_id is not None:
        create_or_update_card_filters(card_id, filters)

    if with_status == True:
        parse_card_certificates_by_card_identifier(card_identifier, card_id)

    return

# ----------------------------------------------------------------------------------------------------------------------

def prepare_certificate_field(key, value):
    value = value.strip(); result = value

    if key in ['created_at', 'closed_at', 'new_closed_at']:
        if value != '':
            result = datetime.datetime.strptime(value, '%d.%m.%Y')
        else:
            result = None

    return result

# ----------------------------------------------------------------------------------------------------------------------

def prepare_value(value):
    result = value.strip()

    for str in [ 'Состояние класса', 'Состояние СвУБ' ]:
        result = re.sub('{0}\:\s+'.format(str), '', result, flags=re.M | re.I)
    result = re.sub('<\/?b>', '', result, flags=re.M | re.I)

    return result

# ----------------------------------------------------------------------------------------------------------------------

def parse_card_certificates_by_card_identifier(card_identifier, card_id=None):
    if card_id is None:
        card_id = get_or_create_card(card_identifier)

    url = 'https://lk.rs-class.org/regbook/status?fleet_id={0}'.format(card_identifier)
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')

    scripts = soup.find_all('script')
    script = scripts[len(scripts)-1]
    matches = re.findall(r"jsonString\s\=\s(.+);", script.text.strip().replace('],]', ']]'), flags=re.M)
    data = json.loads(matches[0].strip())

    aaDataV1 = data['aaDataV1']
    card_states = {
        'card_id': card_id,
        'class': prepare_value(aaDataV1[len(aaDataV1)-2][1]),
        'form_8_1_3': prepare_value(aaDataV1[len(aaDataV1)-1][1])
    }
    create_or_replace_card_states(card_id, [ card_states ])

    card_contacts = {
        'card_id': card_id,
        'operator': prepare_value(data['aaDataV2'][0][2]),
        'address': prepare_value(data['aaDataV2'][2][2]),
        'email': prepare_value(data['aaDataV2'][3][2]),
        'cite': prepare_value(data['aaDataV2'][4][2])
    }
    create_or_replace_card_contacts(card_id, [ card_contacts ])

    db_columns = [ 'e_cert', 'type', 'name', 'code', 'created_at', 'closed_at', 'new_closed_at', 'state' ]
    item_index = 0; card_certificates = list()
    for item in data['aaDataS0']:
        certificate = { 'card_id': card_id }; db_column_index = 0; column_index = 0
        for column in data['aoColumnsS0']:
            is_db_column = db_column_index < len(db_columns)
            field = db_columns[db_column_index] if is_db_column else ''
            value = prepare_certificate_field(field, item[column_index]) if is_db_column else ''

            if 'visible' in column.keys():
                if column['visible'] == True and is_db_column:
                    certificate[field] = value; db_column_index += 1
            elif is_db_column:
                certificate[field] = value; db_column_index += 1
            column_index += 1

        card_certificates.append(certificate)
        item_index += 1

    create_or_replace_card_certificates(card_id, card_certificates)

    return

# ----------------------------------------------------------------------------------------------------------------------

# Спарсить все карточки, удовлетворяющие условиям соответствующих фильтров
# filters [ [
#   name,   # название фильтра для составление запроса
#   value,  # значение фильтра
#   field   # название поля под фильтр в таблице связей cards_filters
# ], ]
def parse_card_by_filters(filters, level=1):
    data = {}
    for filter in filters:
        data[filter['name']] = filter['value']

    url = 'https://lk.rs-class.org/regbook/regbookVessel'
    headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
    response = requests.post(url, data=data, headers=headers)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    tr_index = 0
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        tr_length = len(tds)
        if tr_length == 0: continue
        last_column = tds[tr_length-1]

        with_status = False; identifier = None
        links = last_column.find_all('a')
        for link in links:
            href = link.get('href')
            matches = re.findall(r'(vessel|status)\?fleet_id=(\d+)', href)
            action = matches[0][0]
            identifier = matches[0][1]
            with_status = with_status or (action == 'status')
            if action != 'status' and action != 'vessel':
                print('ЕСТЬ ЕЩЕ ССЫЛКА ДЛЯ ПАРСИНГА', href)

        card_time_start = datetime.datetime.now()
        print_start_status('[{0}] Карточка #{1}'.format(tr_index+1, identifier), level)
        parse_card_by_identifier(identifier, filters, with_status)
        print_end_status(card_time_start, level)
        tr_index += 1

    return

# ----------------------------------------------------------------------------------------------------------------------

total_start_time = datetime.datetime.now()
print_start_status('Начало работы скрипта', 0)

if sys.argv[1] == '1':
    test_start_time = datetime.datetime.now()
    print_start_status('Спарсить тестовые карточки по номеру ИМО')
    index = 0
    for identifier in ['1017605', '990745']:
        card_time_start = datetime.datetime.now()
        print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        parse_card_by_identifier(identifier)
        print_end_status(card_time_start, 2)
        index += 1
    print_end_status(test_start_time)

elif sys.argv[1] == '2':
    test_start_time = datetime.datetime.now()
    print_start_status('Сравнить тестовые карточки с сайта с карточками из БД')
    index = 0
    for identifier in ['1017605', '990745']:
        card_time_start = datetime.datetime.now()
        print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        card = Card(identifier)
        card.cmp_with_cite()
        print_end_status(card_time_start, 2)
        index += 1
    print_end_status(test_start_time)

elif sys.argv[1] == '3':
    test_start_time = datetime.datetime.now()
    print_start_status('Выгрузить тестовые карточки из БД в формате .csv')
    index = 0
    for identifier in ['1017605', '990745']:
        card_time_start = datetime.datetime.now()
        print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        card = Card(identifier)
        card.export()
        print_end_status(card_time_start, 2)
        index += 1
    print_end_status(test_start_time)

elif sys.argv[1] == '4':
#     print_start_status('Спарсить карточки по фильтру: Россия, Владивосток, Научно-исследовательские')
#     parse_card_by_filters([
#           { 'name': 'gorodRegbook', 'value': '0E224C4F-DE2B-4DD6-AB9B-B6BBABB7B7C4', 'field': 'filter_city_identifier' },
#           { 'name': 'countryId', 'value': '6CF1E5F4-2B6D-4DC6-836B-287154684870', 'field': 'filter_country_identifier' },
#           { 'name': 'statgr', 'value': '47488F18-691C-AD5D-0A1C-9EA637E43848', 'field': 'filter_type_identifier' },
#     ], 2)

    print_start_status('Спарсить карточки по фильтру: Панама, Панама, Нефтеналивные')
    parse_card_by_filters([
        { 'name': 'gorodRegbook', 'value': 'DD1212EB-494D-41E4-A54A-B914845826A1', 'field': 'filter_city_identifier' },
        { 'name': 'countryId', 'value': 'D3339EB0-B3A8-461F-8493-DE358CAB09C7', 'field': 'filter_country_identifier' },
        { 'name': 'statgr', 'value': 'F188B3EF-E54D-D82E-6F79-AD7D9A4A4CCD', 'field': 'filter_type_identifier' },
    ], 2)

elif sys.argv[1] == '5':
    test_start_time = datetime.datetime.now()
    print_start_status('Спарсить карточки по фильтрам из БД')
#     filters = [
#         Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ], 'filter_city_identifier'),
#         Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ], 'filter_country_identifier'),
#         Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ], 'filter_type_identifier'),
#         Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ], 'filter_class_identifier')
#     ]

    filters = [
        Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ], 'filter_country_identifier'),
    ]
    for filter in filters:
        start_time = datetime.datetime.now()
        print_start_status('Фильтр '+filter.ru_name.upper(), 2)
        for row in filter.get_list():
            parse_card_by_filters([ { 'name': row[0], 'field': row[1], 'value': row[2] } ], 3)
        print_end_status(start_time)
    print_end_status(test_start_time)

print_end_status(total_start_time, 0)
