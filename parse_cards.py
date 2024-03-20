from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen
from decouple import config
from threading import Thread
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
        self.id = None
        self.identifier = identifier

    def get(self):
        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('SELECT id FROM cards WHERE identifier = %s', (self.identifier,))
            results = cursor.fetchall()
            if cursor.rowcount > 0:
                self.id = results[0][0]
        except Exception as e:
            print('[card.get]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

    def create(self):
        if self.id is not None:
            return self.id

        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('INSERT INTO cards (identifier) VALUES (%s)', (self.identifier,))
            self.id = cursor.lastrowid
            connection.commit()
        except Exception as e:
            print('[card.create]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

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
        except Exception as e:
            print('[card.get_data]', e)
        finally:
            cursor.close()
            connection.close()

        return result

    # Сравнить значения характеристик из БД с сайтом
    def cmp_with_cite(self):
        db_card = self.get_data()

        url = f'https://lk.rs-class.org/regbook/vessel?fleet_id={self.identifier}&a=print'
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
                print_start_status('Карточка #'+self.identifier+': совпадение не найдено', 3)

            success = success and is_exist

        print_start_status(('успех' if success == True else 'провал'), 3)

        return

    # Выгрузить характеристики из БД в файл формата .csv
    def export(self):
        file = open('cards/card_'+self.identifier+'.csv', 'w')
        csv_writer = csv.writer(file)
        for row in self.get_data():
            print(self.identifier, row)
            csv_writer.writerow(row)
        file.close()

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
            sql = f"""
                select '{self.cite_name}' as name, '{self.db_relationship}' as field, identifier as value
                from filter_{self.db_name} order by id
            """
            cursor.execute(sql, ())
            result = cursor.fetchall()
        except Exception as e:
            print('[filter.get_list]', e)
        finally:
            cursor.close()
            connection.close()

        return result

# ----------------------------------------------------------------------------------------------------------------------

class Group:
    def __init__(self, name):
        self.id = None
        self.name = name

    def get(self):
        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('SELECT id FROM group_properties WHERE name = %s', (self.name,))
            results = cursor.fetchall()

            if cursor.rowcount > 0:
                self.id = results[0][0]
        except Exception as e:
            print('[group.get]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

    def create(self):
        if self.id is not None:
            return self.id

        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('INSERT INTO group_properties (name) VALUES (%s)', (self.name,))
            self.id = cursor.lastrowid
            connection.commit()
        except Exception as e:
            print('[card.create]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

# Сохранить в БД, если группа характеристик новая, вернуть id группы характеристик
def get_or_create_group(name):
    group = Group(name)
    group_id = group.get()

    if group_id == None:
        try:
            group_id = group.create()
        except e:
            group_id = group.get()

    return group_id

# ----------------------------------------------------------------------------------------------------------------------

class Property:
    def __init__(self, name, group_id):
        self.id = None
        self.name = name
        self.group_id = group_id

    def get(self):
        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('SELECT id FROM properties WHERE name = %s AND group_id = %s', (self.name, self.group_id))
            results = cursor.fetchall()

            if cursor.rowcount > 0:
                self.id = results[0][0]
        except Exception as e:
            print('[property.get]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

    def create(self):
        connection = get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('INSERT INTO properties (name, group_id) VALUES (%s, %s)', (self.name, self.group_id))
            self.id = cursor.lastrowid
            connection.commit()
        except Exception as e:
            print('[property.create]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

# Сохранить в БД, если характеристика новая, вернуть id характеристики
def get_or_create_property(name, group_id):
    property = Property(name, group_id)
    property_id = property.get()

    if property_id == None:
        try:
            property_id = property.create()
        except:
            property_id = property.get()

    return property_id

# ----------------------------------------------------------------------------------------------------------------------

# Сохранить в БД, если карточка судна новая, вернуть id карточки судна
def get_or_create_card(identifier):
    card = Card(identifier)
    card_id = card.get()

    if card_id == None:
        try:
            card_id = card.create()
        except:
            card_id = card.get()

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
        sql = """
            INSERT INTO cards_properties (card_id, property_id, property_value, updated_at) VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE property_value = %s, updated_at = %s
        """
        cursor.execute(sql, (card_id, property_id, property_value, ts, property_value, ts))
        connection.commit()
    except Exception as e:
        print('[create_or_update_card_property]', e)
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
    except Exception as e:
        print('[create_or_update_card_filters]', e)
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
        cursor.execute(f'DELETE FROM {db_name} WHERE card_id = %s order by id', (card_id,))
        connection.commit()
        for record in records:
            columns = list(record.keys())
            query_columns = ', '.join(columns)
            query_values = ', '.join(map(lambda column: '%s', columns))
            params = list(record.values())
            sql = f'INSERT INTO {db_name} ({query_columns}) VALUES ({query_values})'
            cursor.execute(sql, params)
            connection.commit()
    except Exception as e:
        print(f'[create_or_replace_card_{db_name}]', e)
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
    if duration // 60 <= 1:
        print('[{}]'.format(datetime.datetime.now()), separator, caption, round(duration, 2), '(s)')
    else:
        duration = duration / 60.0
        print('[{}]'.format(datetime.datetime.now()), separator, caption, round(duration, 2), '(m)')

    return

# ----------------------------------------------------------------------------------------------------------------------

# Спарсить карточку с характеристиками судна
def parse_card_by_identifier(card_identifier, filters=None, with_status=False):
    card_id = get_or_create_card(card_identifier)

    url = f'https://lk.rs-class.org/regbook/vessel?fleet_id={card_identifier}&a=print'
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

    url = f'https://lk.rs-class.org/regbook/status?fleet_id={card_identifier}'
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
def parse_card_by_filters(filters, level=1, thead=-1):
    data = {}
    for filter in filters: data[filter['name']] = filter['value']

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
        print_start_status(f'[{tr_index+1}] Карточка #{identifier}', level)
        parse_card_by_identifier(identifier, filters, with_status)
        print_end_status(card_time_start, level)
        tr_index += 1

    return

# ----------------------------------------------------------------------------------------------------------------------

def sequential(trs, thead, filters, level):
    thead_time_start = datetime.datetime.now()
    thead_caption = f'[thead #{thead}][{len(trs)} шт]'
    for tr in trs:
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
        caption = f'[thead #{thead}] Карточка #{identifier}'
#         print_start_status(caption, level)
        parse_card_by_identifier(identifier, filters, with_status)
        print_end_status(card_time_start, level, caption)
    print_end_status(thead_time_start, level+1, thead_caption)

    return

# Спарсить все карточки, удовлетворяющие условиям соответствующих фильтров
# filters [ [
#   name,   # название фильтра для составление запроса
#   value,  # значение фильтра
#   field   # название поля под фильтр в таблице связей cards_filters
# ], ]
def parse_card_by_filters__threads(filters, level=1, force=False):
    data = {}
    for filter in filters: data[filter['name']] = filter['value']

    url = 'https://lk.rs-class.org/regbook/regbookVessel'
    headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
    response = requests.post(url, data=data, headers=headers)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    trs = soup.find_all('tr'); n_proc = 10; n_trs = len(trs)
    n = int(n_trs / n_proc) if n_trs % n_proc == 0 else int(n_trs // n_proc + 1)
    if n_trs > 0:
#         print_start_status(', '.join(map(lambda x: x['name'] + ':' + x['value'], filters)), level)
        print_start_status(f'Max кол-во потоков: {n_proc} | Кол-во карточек: {n_trs} | Кол-во карточек на поток: {n}', level)

    if force == True:
        threads = []; thead_index = 1
        for trs_chunk in [trs[i:i+n] for i in range(0, n_trs, n)]:
            t = Thread(target=sequential, args=(trs_chunk, thead_index, filters, level)); thead_index += 1
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

    return n_trs

# ======================================================================================================================

def command__parse_test_cards_by_identifier(caption='Спарсить тестовые карточки идентификатору'):
    test_start_time = datetime.datetime.now()
    print_start_status(caption)
    index = 0
    for identifier in ['1017605', '990745']:
        card_time_start = datetime.datetime.now()
        print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        parse_card_by_identifier(identifier)
        print_end_status(card_time_start, 2)
        index += 1
    print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__cmp_test_cards_with_cite_cards(caption='Сравнить тестовые карточки с сайта с карточками из БД'):
    test_start_time = datetime.datetime.now()
    print_start_status(caption)
    index = 0
    for identifier in ['1017605', '990745']:
        card_time_start = datetime.datetime.now()
        print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        card = Card(identifier); card.cmp_with_cite()
        print_end_status(card_time_start, 2)
        index += 1
    print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__export_test_cards(caption='Выгрузить тестовые карточки из БД в формате .csv'):
    test_start_time = datetime.datetime.now()
    print_start_status(caption)
    index = 0
    for identifier in ['990745', '1017605']:
        card_time_start = datetime.datetime.now()
        print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        card = Card(identifier); card.export()
        print_end_status(card_time_start, 2)
        index += 1
    print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_cards_by_custom_filters():
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

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_cards_by_db_filters(caption='Спарсить карточки по фильтрам из БД'):
    test_start_time = datetime.datetime.now()
    print_start_status(caption)
    filters = [
        Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ], 'filter_city_identifier'),
        Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ], 'filter_country_identifier'),
        Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ], 'filter_type_identifier'),
        Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ], 'filter_class_identifier')
    ]
    for filter in filters:
        start_time = datetime.datetime.now()
        print_start_status('Фильтр '+filter.ru_name.upper(), 2)
        for row in filter.get_list():
            parse_card_by_filters([ { 'name': row[0], 'field': row[1], 'value': row[2] } ], 3)
        print_end_status(start_time)
    print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_cards_by_custom_filters__threads():
    print_start_status('Спарсить карточки ПОТОКАМИ по фильтру: Панама, Панама')
    parse_card_by_filters__threads([
        { 'name': 'gorodRegbook', 'value': 'DD1212EB-494D-41E4-A54A-B914845826A1', 'field': 'filter_city_identifier' },
        { 'name': 'countryId', 'value': 'D3339EB0-B3A8-461F-8493-DE358CAB09C7', 'field': 'filter_country_identifier' },
    ], 2)

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_cards_by_db_filters__threads(caption='Спарсить карточки ПОТОКАМИ по фильтрам из БД'):
    filters = []
    match sys.argv[2]:
        case 'cities':
            filters = [Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ], 'filter_city_identifier')]
        case 'countries':
            filters = [Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ], 'filter_country_identifier')]
        case 'types':
            filters = [Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ], 'filter_type_identifier')]
        case 'classes':
            filters = [Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ], 'filter_class_identifier')]
        case 'all':
            filters = [
                Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ], 'filter_city_identifier'),
                Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ], 'filter_country_identifier'),
                Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ], 'filter_type_identifier'),
                Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ], 'filter_class_identifier')
            ]
        case _:
            print(f'Фильтра "{sys.argv[2]}" не существует')

    test_start_time = datetime.datetime.now()
    print_start_status(caption)
    for filter in filters:
        start_time = datetime.datetime.now()
        filter_values = filter.get_list()
        caption = f'Фильтр {filter.ru_name.upper()} ({filter_values} шт. значений)'
        print_start_status(caption, 2)
        n_cards = 0
        for item in filter_values:
            n_cards += parse_card_by_filters__threads([ { 'name': item[0], 'field': item[1], 'value': item[2] } ], 3)
        print_end_status(start_time, 1, caption + f': {n_cards} шт. карточек')
    print_end_status(test_start_time)

# ======================================================================================================================

commands = [
    { 'code': '1', 'caption': 'Спарсить тестовые карточки по номеру ИМО' },
    { 'code': '2', 'caption': 'Сравнить тестовые карточки с сайта с карточками из БД' },
    { 'code': '3', 'caption': 'Выгрузить тестовые карточки из БД в формате .csv' },
    { 'code': '4', 'caption': 'Спарсить карточки по фильтру: Панама, Панама, Нефтеналивные' },
    { 'code': '5', 'caption': 'Спарсить карточки по фильтрам из БД' },
    { 'code': '6', 'caption': 'Спарсить карточки ПОТОКАМИ по фильтру: Панама, Панама' },
    { 'code': '7', 'caption': 'Спарсить карточки ПОТОКАМИ по фильтрам из БД' },
]

def main():
    if len(sys.argv) < 2:
        print('Укажите один из возможных режимов запуска скрипта:')
        for command in commands:
            code = command['code']
            caption = command['caption']
            print(f'{code} - {caption}')
        return

    if sys.argv[1] == '7' and len(sys.argv) < 3:
        print('Укажите один из возможных вариантов фильтра карточек: cities, countries, types, classes, all.')
        return

    func_name = ''
    match sys.argv[1]:
        case '1':
            func_name = 'command__parse_test_cards_by_identifier'
        case '2':
            func_name = 'command__cmp_test_cards_with_cite_cards'
        case '3':
            func_name = 'command__export_test_cards'
        case '4':
            func_name = 'command__parse_cards_by_custom_filters'
        case '5':
            func_name = 'command__parse_cards_by_db_filters'
        case '6':
            func_name = 'command__parse_cards_by_custom_filters__threads'
        case '7':
            func_name = 'command__parse_cards_by_db_filters__threads'
        case _:
            print(f'Команды с кодом "{sys.argv[1]}" не существует')
            return

    if func_name == '':
        print(f'Команды с кодом "{sys.argv[1]}" не существует')
        return

    total_start_time = datetime.datetime.now()
    print_start_status('Начало работы скрипта', 0)

    eval(func_name + "()")

    print_end_status(total_start_time, 0)

if __name__ == "__main__":
    main()
