import sys
import datetime

from models.card import Card
from models.parser import Parser
from models.card_parser import Card_Parser
from models.logger import Logger


# ======================================================================================================================

def command__parse_test_cards_by_identifier(caption='Спарсить тестовые карточки идентификатору'):
    test_start_time = datetime.datetime.now()
    Logger().print_start_status(caption)
    index = 0
    for identifier in ['1017605', '990745']:
        card_time_start = datetime.datetime.now()
        Logger().print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        Card_Parser(identifier).parse()
        Logger().print_end_status(card_time_start, 2)
        index += 1
    Logger().print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__cmp_test_cards_with_cite_cards(caption='Сравнить тестовые карточки с сайта с карточками из БД'):
    test_start_time = datetime.datetime.now()
    Logger().print_start_status(caption)
    index = 0
    for identifier in ['1017605', '990745']:
        card_time_start = datetime.datetime.now()
        Logger().print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        card = Card(identifier); card.cmp_with_cite()
        Logger().print_end_status(card_time_start, 2)
        index += 1
    Logger().print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__export_test_cards(caption='Выгрузить тестовые карточки из БД в формате .csv'):
    test_start_time = datetime.datetime.now()
    Logger().print_start_status(caption)
    index = 0
    for identifier in ['990745', '1017605']:
        card_time_start = datetime.datetime.now()
        Logger().print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        card = Card(identifier); card.export()
        Logger().print_end_status(card_time_start, 2)
        index += 1
    Logger().print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_cards_by_custom_filters():
#     Logger().print_start_status('Спарсить карточки по фильтру: Россия, Владивосток, Научно-исследовательские')
#     parse_card_by_filters([
#           { 'name': 'gorodRegbook', 'value': '0E224C4F-DE2B-4DD6-AB9B-B6BBABB7B7C4', 'field': 'filter_city_identifier' },
#           { 'name': 'countryId', 'value': '6CF1E5F4-2B6D-4DC6-836B-287154684870', 'field': 'filter_country_identifier' },
#           { 'name': 'statgr', 'value': '47488F18-691C-AD5D-0A1C-9EA637E43848', 'field': 'filter_type_identifier' },
#     ], 2)

    Logger().print_start_status('Спарсить карточки по фильтру: Панама, Панама, Нефтеналивные')
    Parser().parse([
        { 'name': 'gorodRegbook', 'value': 'DD1212EB-494D-41E4-A54A-B914845826A1', 'field': 'filter_city_identifier' },
        { 'name': 'countryId', 'value': 'D3339EB0-B3A8-461F-8493-DE358CAB09C7', 'field': 'filter_country_identifier' },
        { 'name': 'statgr', 'value': 'F188B3EF-E54D-D82E-6F79-AD7D9A4A4CCD', 'field': 'filter_type_identifier' },
    ], 2)

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_cards_by_db_filters(caption='Спарсить карточки по фильтрам из БД'):
    test_start_time = datetime.datetime.now()
    Logger().print_start_status(caption)
    filters = [
        Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ], 'filter_city_identifier'),
        Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ], 'filter_country_identifier'),
        Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ], 'filter_type_identifier'),
        Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ], 'filter_class_identifier')
    ]
    for filter in filters:
        start_time = datetime.datetime.now()
        Logger().print_start_status('Фильтр '+filter.ru_name.upper(), 2)
        for row in filter.get_list():
            Parser().parse([ { 'name': row[0], 'field': row[1], 'value': row[2] } ], 3)
        Logger().print_end_status(start_time)
    Logger().print_end_status(test_start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_cards_by_custom_filters__threads():
    Logger().print_start_status('Спарсить карточки ПОТОКАМИ по фильтру: Панама, Панама')
    Parser().parse_with_threads([
        { 'name': 'gorodRegbook', 'value': 'DD1212EB-494D-41E4-A54A-B914845826A1', 'field': 'filter_city_identifier' },
        { 'name': 'countryId', 'value': 'D3339EB0-B3A8-461F-8493-DE358CAB09C7', 'field': 'filter_country_identifier' },
    ], 2, True)

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
    Logger().print_start_status(caption)
    for filter in filters:
        start_time = datetime.datetime.now()
        filter_values = filter.get_list()
        caption = f'Фильтр {filter.ru_name.upper()} ({filter_values} шт. значений)'
        Logger().print_start_status(caption, 2)
        n_cards = 0
        for item in filter_values:
            n_cards += Parser().parse_with_threads([ { 'name': item[0], 'field': item[1], 'value': item[2] } ], 3, True)
        Logger().print_end_status(start_time, 1, caption + f': {n_cards} шт. карточек')
    Logger().print_end_status(test_start_time)

# ======================================================================================================================

commands = [
    { 'code': '1', 'caption': 'Спарсить тестовые карточки по идентификатору' },
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
    Logger().print_start_status('Начало работы скрипта', 0)

    eval(func_name + "()")

    Logger().print_end_status(total_start_time, 0)

if __name__ == "__main__":
    main()
