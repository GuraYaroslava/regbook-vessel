import sys
import datetime
import db_connector
import multiprocessing

from models.card import Card
from models.parser import Parser
from models.filter import Filter
from models.card_parser import Card_Parser
from models.logger import Logger


# Получить фильтры
# @param {0|1} mode Если режим = 1, возвращать список объектов, иначе список моделей
# @todo Вынести возможные данные о фильтрах в файл формата .csv, чтобы не хардкодить в скрипте
def get_filters(mode=0):
    init_filters = [
        { 'ru_name': 'Города', 'cite_name': 'gorodRegbook', 'db_name': 'cities', 'db_columns': [ 'identifier', 'name', 'name_eng', 'country_ru' ] },
        { 'ru_name': 'Страны', 'cite_name': 'countryId', 'db_name': 'countries', 'db_columns': [ 'identifier', 'name', 'name_eng' ] },
        { 'ru_name': 'Статистические группы судов', 'cite_name': 'statgr', 'db_name': 'types', 'db_columns': [ 'identifier', 'code', 'name', 'name_eng' ] },
        { 'ru_name': 'Ледовые категории', 'cite_name': 'icecat', 'db_name': 'classes', 'db_columns': [ 'identifier', 'name' ] }
    ]

    if mode == 1:
        return init_filters

    filters = []
    for args in init_filters:
        filter = Filter(); filter.set_attrs(args)
        filters.append(filter)

    return filters

# ======================================================================================================================

def command__init_schema(caption='Создать схему БД'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now()
    db_connector.init_schema()
    Logger().print_end_status(start_time)

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_filters(caption='Спарсить фильтры'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now()
    filters = get_filters()
    for filter in filters:
        filter_start_time = datetime.datetime.now()
        Logger().print_start_status('Фильтр '+filter.ru_name.upper(), 2)
        filter.parse()
        Logger().print_end_status(filter_start_time, 3)
    Logger().print_end_status(start_time, 1, caption)

    return

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_filters__threads(caption='Спарсить фильтры потоками'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now()
    filters = get_filters()
    for filter in filters:
        filter.start()
    # Подождем, пока все потоки завершат свою работу
    for filter in filters:
        filter.join()
    for filter in filters:
        Logger().print_start_status('Фильтр '+filter.ru_name.upper()+f': {filter.parse_duration()} (s)', 2)
    Logger().print_end_status(start_time, 1, caption)

    return

# ----------------------------------------------------------------------------------------------------------------------

def command__parse_filters__multiprocess___sequential(filter_chunk, proc):
    for filter_args in filter_chunk:
        start_time = datetime.datetime.now()
        filter = Filter(); filter.set_attrs(filter_args); filter.parse()
        Logger().print_end_status(start_time, 2, f'[proc #{proc}] Фильтр '+filter.ru_name.upper())

def command__parse_filters__multiprocess(caption='Спарсить фильтры мультипроцессорно'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now()

    init_filters = get_filters(1)
    n_proc = multiprocessing.cpu_count()
    n_filter = len(init_filters)
    n = int(n_filter / n_proc) if n_filter % n_proc == 0 else int(n_filter // n_proc + 1)
    print(f'Кол-во ядер: {n_proc} |', f'Кол-во фильтров: {n_filter} |', f'Кол-во фильтров на ядро: {n}')

    init = []; index = 1
    for filter_chunk in [init_filters[i:i + n] for i in range(0, n_filter, n)]:
        init.append((filter_chunk, index)); index += 1
    with multiprocessing.Pool() as pool:
       pool.starmap(command__parse_filters__multiprocess___sequential, init)
    Logger().print_end_status(start_time, 1, caption)

    return

# ----------------------------------------------------------------------------------------------------------------------

# @todo Избавиться от хардкода, вынести идентификаторы тестовых карточек в файл .csv, напрмиер
def command__parse_test_cards_by_identifier(caption='Спарсить тестовые карточки по идентификатору'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now(); index = 0
    for identifier in ['990436', '1017605', '990745']:
        card_start_time = datetime.datetime.now()
        Logger().print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        Card_Parser(identifier).parse()
        Logger().print_end_status(card_start_time, 2); index += 1
    Logger().print_end_status(start_time)

# ----------------------------------------------------------------------------------------------------------------------

# @todo Избавиться от хардкода, вынести идентификаторы тестовых карточек в файл .csv, напрмиер
def command__cmp_test_cards_with_cite_cards(caption='Сравнить тестовые карточки с сайта с карточками из БД'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now(); index = 0
    for identifier in ['1017605', '990745']:
        card_start_time = datetime.datetime.now()
        Logger().print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        Card(identifier).cmp_with_cite()
        Logger().print_end_status(card_start_time, 2); index += 1
    Logger().print_end_status(start_time)

# ----------------------------------------------------------------------------------------------------------------------

# @todo Избавиться от хардкода, вынести идентификаторы тестовых карточек в файл .csv, напрмиер
def command__export_test_cards(caption='Выгрузить тестовые карточки из БД в формате .csv'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now(); index = 0
    for identifier in ['990745', '1017605']:
        card_time_start = datetime.datetime.now()
        Logger().print_start_status('[{0}] Карточка #{1}'.format(index+1, identifier), 2)
        Card(identifier).export()
        Logger().print_end_status(card_time_start, 2); index += 1
    Logger().print_end_status(start_time)

# ----------------------------------------------------------------------------------------------------------------------

# @todo Избавиться от хардкода
def command__parse_cards_by_custom_filters(mode=0):
    caption = ''; params = []

    match mode:
        case 0:
            caption = 'Спарсить карточки по фильтру: Россия, Владивосток, Научно-исследовательские'
            params = [
                { 'name': 'gorodRegbook', 'value': '0E224C4F-DE2B-4DD6-AB9B-B6BBABB7B7C4', 'field': 'filter_city_identifier' },
                { 'name': 'countryId', 'value': '6CF1E5F4-2B6D-4DC6-836B-287154684870', 'field': 'filter_country_identifier' },
                { 'name': 'statgr', 'value': '47488F18-691C-AD5D-0A1C-9EA637E43848', 'field': 'filter_type_identifier' },
            ]
        case 1:
            caption = 'Спарсить карточки по фильтру: Панама, Панама, Нефтеналивные'
            params = [
                { 'name': 'gorodRegbook', 'value': 'DD1212EB-494D-41E4-A54A-B914845826A1', 'field': 'filter_city_identifier' },
                { 'name': 'countryId', 'value': 'D3339EB0-B3A8-461F-8493-DE358CAB09C7', 'field': 'filter_country_identifier' },
                { 'name': 'statgr', 'value': 'F188B3EF-E54D-D82E-6F79-AD7D9A4A4CCD', 'field': 'filter_type_identifier' },
            ]

    Logger().print_start_status(caption); start_time = datetime.datetime.now()
    Parser().parse(params, 2)
    Logger().print_end_status(start_time)

# ----------------------------------------------------------------------------------------------------------------------

# @todo Избавиться от хардкода
def command__parse_cards_by_db_filters(caption='Спарсить карточки по фильтрам из БД'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now()
    filters = [
        Filter('Города', 'gorodRegbook', 'cities', [ 'identifier', 'name', 'name_eng', 'country_ru' ], 'filter_city_identifier'),
        Filter('Страны', 'countryId', 'countries', [ 'identifier', 'name', 'name_eng' ], 'filter_country_identifier'),
        Filter('Статистические группы судов', 'statgr', 'types', [ 'identifier', 'code', 'name', 'name_eng' ], 'filter_type_identifier'),
        Filter('Ледовые категории', 'icecat', 'classes', [ 'identifier', 'name' ], 'filter_class_identifier')
    ]
    for filter in filters:
        filter_start_time = datetime.datetime.now()
        Logger().print_start_status('Фильтр '+filter.ru_name.upper(), 2)
        for row in filter.get_list():
            Parser().parse([ { 'name': row[0], 'field': row[1], 'value': row[2] } ], 3)
        Logger().print_end_status(filter_start_time)
    Logger().print_end_status(start_time)

# ----------------------------------------------------------------------------------------------------------------------

# @todo Избавиться от хардкода
def command__parse_cards_by_custom_filters__threads(caption='Спарсить карточки ПОТОКАМИ по фильтру: Панама, Панама'):
    Logger().print_start_status(caption); start_time = datetime.datetime.now()
    Parser().parse_with_threads([
        { 'name': 'gorodRegbook', 'value': 'DD1212EB-494D-41E4-A54A-B914845826A1', 'field': 'filter_city_identifier' },
        { 'name': 'countryId', 'value': 'D3339EB0-B3A8-461F-8493-DE358CAB09C7', 'field': 'filter_country_identifier' },
    ], 2, True)
    Logger().print_end_status(start_time)

# ----------------------------------------------------------------------------------------------------------------------

# @todo Избавиться от хардкода
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

    Logger().print_start_status(caption); start_time = datetime.datetime.now()
    for filter in filters:
        filter_start_time = datetime.datetime.now()
        filter_values = filter.get_list()
        caption = f'Фильтр {filter.ru_name.upper()} ({filter_values} шт. значений)'
        Logger().print_start_status(caption, 2)
        n_cards = 0
        for item in filter_values:
            n_cards += Parser().parse_with_threads([ { 'name': item[0], 'field': item[1], 'value': item[2] } ], 3, True)
        Logger().print_end_status(filter_start_time, 1, caption + f': {n_cards} шт. карточек')
    Logger().print_end_status(start_time)

# ======================================================================================================================

commands = [
    { 'code': '1', 'caption': 'Создать схему БД', 'name': 'command__init_schema' },
    { 'code': '2', 'caption': 'Спарсить фильтры', 'name': 'command__parse_filters' },
    { 'code': '3', 'caption': 'Спарсить фильтры потоками', 'name': 'command__parse_filters__threads' },
    { 'code': '4', 'caption': 'Спарсить фильтры мультипроцессорно', 'name': 'command__parse_filters__multiprocess' },
    { 'code': '5', 'caption': 'Спарсить тестовые карточки по идентификатору', 'name': 'command__parse_test_cards_by_identifier' },
    { 'code': '6', 'caption': 'Сравнить тестовые карточки с сайта с карточками из БД', 'name': 'command__cmp_test_cards_with_cite_cards' },
    { 'code': '7', 'caption': 'Выгрузить тестовые карточки из БД в формате .csv', 'name': 'command__export_test_cards' },
    { 'code': '8', 'caption': 'Спарсить карточки по фильтру: Панама, Панама, Нефтеналивные', 'name': 'command__parse_cards_by_custom_filters' },
    { 'code': '9', 'caption': 'Спарсить карточки по фильтрам из БД', 'name': 'command__parse_cards_by_db_filters' },
    { 'code': '10', 'caption': 'Спарсить карточки ПОТОКАМИ по фильтру: Панама, Панама', 'name': 'command__parse_cards_by_custom_filters__threads' },
    { 'code': '11', 'caption': 'Спарсить карточки ПОТОКАМИ по фильтрам из БД', 'name': 'command__parse_cards_by_db_filters__threads' },
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
    for command in commands:
        if sys.argv[1] == command['code']:
            func_name = command['name']

    if func_name == '':
        print(f'Команды с кодом "{sys.argv[1]}" не существует')
        return

    total_start_time = datetime.datetime.now()
    Logger().print_start_status('Начало работы скрипта', 0)

    eval(func_name + "()")

    Logger().print_end_status(total_start_time, 0)

if __name__ == "__main__":
    main()
