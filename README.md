# regbook-vessel

Требуется написать парсер [Российского морского регистра судоходства (РС, Регистр)](https://rs-class.org/ru/register/about/)
для создания локальной базы данных ["РЕГИСТРОВАЯ КНИГА"](https://lk.rs-class.org/regbook/regbookVessel).

---
# Проектирование

Фильтры:

| # | Название                                        | Пример                                                                                                      |
|---|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| 1 | Порт приписки / Города / Cities                 | [Ссылка на форму выбора города.](https://lk.rs-class.org/regbook/getDictionary2?d=gorodRegbook&f=formfield) |
| 2 | Флаг / Страны / Countries                       | [Ссылка на форму выбора страны.](https://lk.rs-class.org/regbook/getDictionary2?d=countryId&f=formfield)    |
| 3 | Тип судна / Статистические группы судов / Types | [Ссылка на форму выбора типа судна.](https://lk.rs-class.org/regbook/getDictionary2?d=statgr&f=formfield)   |
| 4 | Ледовый класс / Ледовая категория / Classes     | [Ссылка на форму выбора класса.](https://lk.rs-class.org/regbook/getDictionary2?d=icecat&f=formfield)       |

Карточка:

| #  | Название               | Пример                                                                                                                  |
|----|------------------------|-------------------------------------------------------------------------------------------------------------------------|
| 1  | Предпросмотр           | [Ссылка на предпросмотр характеристик судна](https://lk.rs-class.org/regbook/vessel?fleet_id=994121)                    |
| 2  | Выписка                | [Ссылка на все характеристики судна](https://lk.rs-class.org/regbook/vessel?fleet_id=1017605&a=print)                   |
| 3  | Статус / Свидетельства | [Ссылка на свидетельства / контакты / состояния класса и СвУБ](https://lk.rs-class.org/regbook/status?fleet_id=1017605) |

![Схема БД](resources/schema.png?raw=true "Схема БД")

# Разработка

Пример запроса получения списка карточек по фильтру:

```
curl -X POST \
https://lk.rs-class.org/regbook/regbookVessel \
-H 'cache-control: no-cache' \
-H 'content-type: multipart/form-data' \
-F gorodRegbook= \
-F countryId=6CF1E5F4-2B6D-4DC6-836B-287154684870 \
-F icecat= \
-F statgr=47488F18-691C-AD5D-0A1C-9EA637E43848 \
-F namer='петров'
```

---

Установки:

```
sudo apt install python3
sudo apt install python3-pip
pip install mysql-connector-python
pip install python-decouple
```

---

Запуск парсера карточек, где первый аргумент, если

- 1 - Создать схему БД
- 2 - Спарсить фильтры
- 3 - Спарсить фильтры потоками
- 4 - Спарсить фильтры мультипроцессорно
- 5 - Спарсить тестовые карточки по идентификатору
- 6 - Сравнить тестовые карточки с сайта с карточками из БД
- 7 - Выгрузить тестовые карточки из БД в формате .csv
- 8 - Спарсить карточки по фильтру: Панама, Панама, Нефтеналивные
- 9 - Спарсить карточки по фильтрам из БД
- 10 - Спарсить карточки ПОТОКАМИ по фильтру: Панама, Панама
- 11 - Спарсить карточки ПОТОКАМИ по фильтрам из БД

```
python3 run.py 1
```

---

Запустить консоль mysql:

```
mysql -u root -p
```

Получить количественные данные по таблицам:

```mysql
    select 'card_certificates' as table_name, count(*) as total from card_certificates
        UNION ALL
    select 'card_contacts' as table_name, count(*) as total from card_contacts
        UNION ALL
    select 'card_states' as table_name, count(*) as total from card_states
        UNION ALL
    select 'cards' as table_name, count(*) as total from cards
        UNION ALL
    select 'cards_filters' as table_name, count(*) as total from cards_filters
        UNION ALL
    select 'cards_properties' as table_name, count(*) as total from cards_properties
        UNION ALL
    select 'group_properties' as table_name, count(*) as total from group_properties
        UNION ALL
    select 'properties' as table_name, count(*) as total from properties
        UNION ALL
    select 'filter_cities' as table_name, count(*) as total from filter_cities
        UNION ALL
    select 'filter_classes' as table_name, count(*) as total from filter_classes
        UNION ALL
    select 'filter_countries' as table_name, count(*) as total from filter_countries
        UNION ALL
    select 'filter_types' as table_name, count(*) as total from filter_types;
```

Получить характеристики по конкретной карточке судна:

```mysql
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
      cards.identifier = 990436
    order by group_properties.id, properties.id;
```

Карточки, у которых нету характеристики:

```mysql
    select property_id, count(card_id) from cards_properties group by property_id;

    select id, identifier from cards where not id in ( 
        select distinct card_id from cards_properties where property_id = 13
    );
```