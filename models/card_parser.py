from models.card import Card
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
import requests
import json
import re
from models.card import Card
from models.filter import Filter
from models.group import Group
from models.property import Property


class Card_Parser:
    def __init__(self, identifier):
        self.identifier = identifier

    # Спарсить карточку с характеристиками судна
    def parse(self, filters=None, with_status=False):
        card_id = Card(self.identifier).get_or_create()

        url = f'https://lk.rs-class.org/regbook/vessel?fleet_id={self.identifier}&a=print'
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
                group_id = Group(group_name).get_or_create()

            if property_name != '' and group_id is not None:
                property_id = Property(property_name, group_id).get_or_create()

            if property_id is not None and property_value != '':
                card_properties.append({ 'card_id': card_id, 'property_id': property_id, 'property_value': property_value })

        Card().create_or_update_properties(card_properties)

        if filters is not None and card_id is not None:
            Card().create_or_update_filters(filters, card_id)

        if with_status == True:
            self.parse_certificates(card_id)

        return

    # ----------------------------------------------------------------------------------------------------------------------

    def prepare_certificate_field(self, key, value):
        value = value.strip(); result = value

        if key in ['created_at', 'closed_at', 'new_closed_at']:
            if value != '':
                result = datetime.datetime.strptime(value, '%d.%m.%Y')
            else:
                result = None

        return result

    # ----------------------------------------------------------------------------------------------------------------------

    def prepare_value(self, value):
        result = value.strip()

        for str in [ 'Состояние класса', 'Состояние СвУБ' ]:
            result = re.sub('{0}\:\s+'.format(str), '', result, flags=re.M | re.I)
        result = re.sub('<\/?b>', '', result, flags=re.M | re.I)

        return result

    # ----------------------------------------------------------------------------------------------------------------------

    def parse_certificates(self, card_id=None):
        if card_id is None:
            card_id = Card(self.identifier).get_or_create()

        url = f'https://lk.rs-class.org/regbook/status?fleet_id={self.identifier}'
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
            'class': self.prepare_value(aaDataV1[len(aaDataV1)-2][1]),
            'form_8_1_3': self.prepare_value(aaDataV1[len(aaDataV1)-1][1])
        }
        Card().create_or_replace_states([ card_states ], card_id)

        card_contacts = {
            'card_id': card_id,
            'operator': self.prepare_value(data['aaDataV2'][0][2]),
            'address': self.prepare_value(data['aaDataV2'][2][2]),
            'email': self.prepare_value(data['aaDataV2'][3][2]),
            'cite': self.prepare_value(data['aaDataV2'][4][2])
        }
        Card().create_or_replace_contacts([ card_contacts ], card_id)

        db_columns = [ 'e_cert', 'type', 'name', 'code', 'created_at', 'closed_at', 'new_closed_at', 'state' ]
        item_index = 0; card_certificates = list()
        for item in data['aaDataS0']:
            certificate = { 'card_id': card_id }; db_column_index = 0; column_index = 0
            for column in data['aoColumnsS0']:
                is_db_column = db_column_index < len(db_columns)
                field = db_columns[db_column_index] if is_db_column else ''
                value = self.prepare_certificate_field(field, item[column_index]) if is_db_column else ''

                if 'visible' in column.keys():
                    if column['visible'] == True and is_db_column:
                        certificate[field] = value; db_column_index += 1
                elif is_db_column:
                    certificate[field] = value; db_column_index += 1
                column_index += 1

            card_certificates.append(certificate)
            item_index += 1

        Card().create_or_replace_certificates(card_certificates, card_id)

        return
