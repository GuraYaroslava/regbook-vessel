import db_connector

class Property:
    def __init__(self, name, group_id):
        self.id = None
        self.name = name
        self.group_id = group_id

    def get(self):
        connection = db_connector.get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('SELECT id FROM properties WHERE name = %s AND group_id = %s', (self.name, self.group_id))
            results = cursor.fetchall()

            if cursor.rowcount > 0:
                self.id = results[0][0]
        except Exception as e:
            print('[property.get]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

    def create(self):
        connection = db_connector.get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('INSERT INTO properties (name, group_id) VALUES (%s, %s)', (self.name, self.group_id))
            self.id = cursor.lastrowid
            connection.commit()
        finally:
            cursor.close()
            connection.close()

        return self.id

    # Сохранить в БД, если характеристика новая, вернуть id характеристики
    def get_or_create(self):
        property_id = self.get()

        if property_id == None:
            try:
                property_id = self.create()
            except Exception as e:
                property_id = self.get()

        return property_id
