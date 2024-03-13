from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import mysql.connector

def parse_card(identifier):
    card_id = handle_card(identifier)

    url = 'https://lk.rs-class.org/regbook/vessel?fleet_id={0}&a=print'.format(identifier)
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')

    tr_index = 0; trs = soup.find_all('tr')
    group_id = None; group_name = ''; card_properties = list()
    for tr in trs:
        tds = tr.find_all('td'); td_index = 0; property_id = None; property_name = ''; property_value = ''
        for td in tds:
            colspan = td.get('colspan')

            if colspan is not None:
                group_name = td.find('h3').text.strip()
            else:
                if td_index == 0:
                    property_name = td.text.strip()
                else:
                    property_value = td.get_text(separator='\n').strip()

            td_index += 1

        if group_name != '':
            group_id = handle_group(group_name)

        if property_name != '':
            property_id = handle_property(property_name, group_id)

        if property_id != '' and property_id is not None and property_value != '':
            card_properties.append((card_id, property_id, property_value))

        tr_index += 1

    handle_card_properties(card_properties)

    return

def handle_group(name):
    group_id = None

    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'regbook'
    )

    cursor = db.cursor(prepared=True)
    cursor.execute('SELECT id FROM group_properties WHERE name = %s', (name,))
    results = cursor.fetchall()

    if cursor.rowcount == 0:
        sql = 'INSERT INTO group_properties (name) VALUES (%s)'
        cursor.execute(sql, (name,))
        group_id = cursor.lastrowid
        db.commit()
    else:
        group_id = results[0][0]

    cursor.close()
    db.close()

    return group_id

def handle_property(name, group_id):
    property_id = None

    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'regbook'
    )

    cursor = db.cursor(prepared=True)
    cursor.execute('SELECT id FROM properties WHERE name = %s AND group_id = %s', (name, group_id))
    results = cursor.fetchall()

    if cursor.rowcount == 0:
        sql = 'INSERT INTO properties (name, group_id) VALUES (%s, %s)'
        cursor.execute(sql, (name, group_id))
        property_id = cursor.lastrowid
        db.commit()
    else:
        property_id = results[0][0]

    cursor.close()
    db.close()

    return property_id

def handle_card(identifier):
    card_id = None

    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'regbook'
    )

    cursor = db.cursor(prepared=True)
    cursor.execute('SELECT id FROM cards WHERE identifier = %s', (identifier,))
    results = cursor.fetchall()

    if cursor.rowcount == 0:
        sql = 'INSERT INTO cards (identifier) VALUES (%s)'
        cursor.execute(sql, (identifier,))
        card_id = cursor.lastrowid
        db.commit()
    else:
        card_id = results[0][0]

    cursor.close()
    db.close()

    return card_id

def handle_card_properties(properties):
    for property in properties:
        handle_card_property(property)

    return

def handle_card_property(property):
    db = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'regbook'
    )

    cursor = db.cursor(prepared=True)
    cursor.execute('SELECT id FROM cards_properties WHERE card_id = %s AND property_id = %s', (property[0], property[1]))
    results = cursor.fetchall()

    if cursor.rowcount == 0:
        sql = 'INSERT INTO cards_properties (card_id, property_id, property_value) VALUES (%s, %s, %s)'
        cursor.execute(sql, property)
        db.commit()

    cursor.close()
    db.close()

    return

# ----------------------------------------------------------------------------------------------------------------------

print('---> Спарсить карточки')

time_start = datetime.datetime.now()
print('Время старта: ', time_start)

ids = [1017605, 990745]
index = 0
for id in ids:
    card_time_start = datetime.datetime.now()
    parse_card(id)
    card_time_end = datetime.datetime.now()
    print("#", index+1, (card_time_end - card_time_start).total_seconds(), '(s)')
    index += 1

time_end = datetime.datetime.now()
print('Время окончания: ', time_end)

print('Длительность (s): ', (time_end - time_start).total_seconds())
