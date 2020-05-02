class DBConnector():
    def select(self, table_name, filter=None, return_attr=None):
        raise NotImplementedError

    def create(self, table_name, data):
        raise NotImplementedError

    def update(self, table_name, data):
        raise NotImplementedError

    def delete(self, table_name, data):
        raise NotImplementedError

    def clear(self, table_name):
        raise NotImplementedError

class DBConnectorMock(DBConnector):
    def create(self, table_name, data):
        return True, 1
    def update(self, table_name, data):
        return True
    def delete(self, table_name, data):
        return True
    def clear(self, table_name):
        return True