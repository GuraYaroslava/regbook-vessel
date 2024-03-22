import db_connector

class Group:
    def __init__(self, name):
        self.id = None
        self.name = name

    def get(self):
        connection = db_connector.get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('SELECT id FROM group_properties WHERE name = %s', (self.name,))
            results = cursor.fetchall()

            if cursor.rowcount > 0:
                self.id = results[0][0]
        except Exception as e:
            print('[group.get]', e)
        finally:
            cursor.close()
            connection.close()

        return self.id

    def create(self):
        if self.id is not None:
            return self.id

        connection = db_connector.get_db_connection()
        cursor = connection.cursor(prepared=True)

        try:
            cursor.execute('INSERT INTO group_properties (name) VALUES (%s)', (self.name,))
            self.id = cursor.lastrowid
            connection.commit()
        finally:
            cursor.close()
            connection.close()

        return self.id

    # Сохранить в БД; если группа характеристик новая, вернуть id группы характеристик
    def get_or_create(self):
        group_id = self.get()

        if group_id == None:
            try:
                group_id = self.create()
            except Exception as e:
                group_id = self.get()

        return group_id