import mysql.connector
from decouple import config

def get_db_connection(with_db=True):
    host = config('DB_HOST')
    user = config('DB_USERNAME')
    password = config('DB_PASSWORD')
    database = config('DB_DATABASE') if with_db else ''

    return mysql.connector.connect(host=host, user=user, password=password, database=database)

# Удалить существующую БД, если есть и создать новую со всеми таблицами
def init_schema():
    database = config('DB_DATABASE')
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        cursor.execute('DROP DATABASE IF EXISTS {}'.format(database))
        cursor.execute('CREATE DATABASE IF NOT EXISTS {}'.format(database))

        with open('migrations/init_schema.sql', encoding='utf8') as source:
            for result in cursor.execute(source.read(), multi=True):
                print(result.statement)

    except Error as error:
        print(error)

    finally:
        cursor.close()
        connection.close()