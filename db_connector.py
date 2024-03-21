import mysql.connector
from decouple import config

def get_db_connection():
    host = config('DB_HOST')
    user = config('DB_USERNAME')
    password = config('DB_PASSWORD')
    database = config('DB_DATABASE')

    return mysql.connector.connect(host=host, user=user, password=password, database=database)
