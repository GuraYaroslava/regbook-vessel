from threading import Thread
from urllib.request import urlopen
from bs4 import BeautifulSoup
import db_connector
import datetime
import re


class Filter(Thread):
    def __init__(self, ru_name='', cite_name='', db_name='', db_columns=[], db_relationship=''):
        Thread.__init__(self)
        self.ru_name = ru_name
        self.cite_name = cite_name
        self.db_name = db_name
        self.db_columns = db_columns
        self.db_relationship = db_relationship

    # @param args { ru_name, cite_name, db_name, db_columns }
    def set_attrs(self, args):
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

    # Получить список значений фильтра
    def get_list(self):
        connection = db_connector.get_db_connection()
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

    # Добавить записть в таблицу данного фильтра
    # @param dict values
    def add_record(self, values):
        connection = db_connector.get_db_connection()
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
        except Exception as e:
            print('[filter.add_record]', e)
        finally:
            cursor.close()
            connection.close()

        return

#-----------------------------------------------------------------------------------------------------------------------

    # Thread
    def run(self):
        self.start_parsing_time = datetime.datetime.now()
        self.parse()
        self.end_parsing_time = datetime.datetime.now()

    # Вермя работы потока
    def parse_duration(self):
        return (self.end_parsing_time - self.start_parsing_time).total_seconds()

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