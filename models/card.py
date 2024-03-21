import db_connector
import datetime
import csv
from urllib.request import urlopen
from bs4 import BeautifulSoup
from models.logger import Logger


class Card:
    def __init__(self, identifier=''):
        self.id = None
        self.identifier = identifier

    def get(self):
        connection = db_connector.get_db_connection()
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

        connection = db_connector.get_db_connection()
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

    # Сохранить в БД, если карточка судна новая, вернуть id карточки судна
    def get_or_create(self):
        card_id = self.get()

        if card_id == None:
            try:
                card_id = self.create()
            except:
                card_id = self.get()

        return card_id

    def create_or_update_properties(self, properties):
        for property in properties:
            self.create_or_update_property(property)

        return

    # Сохранить в БД, что карточка имеет характеристику с данным значением
    def create_or_update_property(self, property):
        card_id = property['card_id']
        property_id = property['property_id']
        property_value = property['property_value']
        ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

        connection = db_connector.get_db_connection()
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

    # Сохранить в БД, что карточка ищется по данным фильтрам с соответсвующими значениями
    # filters [ {
    #   name,   # название фильтра для составление запроса
    #   value,  # значение фильтра
    #   field   # название поля под фильтр в таблице связей cards_filters
    # }, ]
    def create_or_update_filters(self, filters, card_id=None):
        card_id = self.get_or_create() if card_id is None else card_id
        connection = db_connector.get_db_connection()
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

    # Сохранить, что карточка имеет соответствующие сертификаты
    def create_or_replace_certificates(self, certificates, card_id=None):
        card_id = self.get_or_create() if card_id is None else card_id
        return self.create_or_replace_relationship(certificates, 'card_certificates', card_id)

    # Сохранить, что карточка имеет соответствующие контакты
    def create_or_replace_contacts(self, contacts, card_id=None):
        card_id = self.get_or_create() if card_id is None else card_id
        return self.create_or_replace_relationship(contacts, 'card_contacts', card_id)

    # Сохранить, что карточка имеет соответствующие состояния
    def create_or_replace_states(self, states, card_id=None):
        card_id = self.get_or_create() if card_id is None else card_id
        return self.create_or_replace_relationship(states, 'card_states', card_id)

    def create_or_replace_relationship(self, records, db_name, card_id):
        connection = db_connector.get_db_connection()
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

    # Получить характеристики из БД
    def get_data(self):
        connection = db_connector.get_db_connection()
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
                Logger().print_start_status('Карточка #'+self.identifier+': совпадение не найдено', 3)

            success = success and is_exist

        Logger().print_start_status(('успех' if success == True else 'провал'), 3)

        return

    # Выгрузить характеристики из БД в файл формата .csv
    def export(self):
        file = open('exports/card_'+self.identifier+'.csv', 'w')
        csv_writer = csv.writer(file)
        for row in self.get_data():
            csv_writer.writerow(row)
        file.close()

        return
