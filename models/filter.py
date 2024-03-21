import db_connector

class Filter:
    def __init__(self, ru_name, cite_name, db_name, db_columns, db_relationship):
        self.ru_name = ru_name
        self.cite_name = cite_name
        self.db_name = db_name
        self.db_columns = db_columns
        self.db_relationship = db_relationship

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
