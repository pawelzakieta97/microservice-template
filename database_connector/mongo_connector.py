import pymongo
from database_connector.db_connector import DBConnector

class MongoConnector(DBConnector):
    def __init__(self, url="mongodb+srv://admin:admin@cluster0-jinrj.mongodb.net/test?retryWrites=true&w=majority", db_name='cyanide'):
        self.client = pymongo.MongoClient(url)
        # self.db = self.client[db_name]
        self.db = self.client[db_name]

    def select(self, table_name, filter=None, return_attr=None):
        if filter is None:
            filter = {}
        if return_attr is not None:
            attributes = {}
            for attribute in return_attr:
                attributes[attribute] = 1
        else:
            attributes = None
        for key, value in filter.items():
            if type(value).__name__ == 'list':
                filter[key] = {"$in": value}
        return [row for row in self.db[table_name].find(filter, attributes)]

    def create(self, table_name, data):
        self.db[table_name].insert_one(data)

    def reinit(self, table_name, data):
        self.drop(table_name)
        self.db[table_name].insert_many(data)

    def drop(self, table_name):
        self.db[table_name].drop()

if __name__ == '__main__':
    db = MongoConnector()
    reservation_data = [{'_id':1, 'date': '2020-06-01', 'time':'dinner', 'restaurant_id':1, 'customer_id':1},
                        {'_id':2, 'date': '2020-06-02', 'time':'dinner', 'restaurant_id':2, 'customer_id':2},
                        {'_id':3, 'date': '2020-06-03', 'time':'dinner', 'restaurant_id':3, 'customer_id':3},
                        {'_id':4, 'date': '2020-06-04', 'time':'dinner', 'restaurant_id':4, 'customer_id':4},]
    db.reinit('reservation', reservation_data)
    print(db.select('reservation', filter={'_id': [3, 2]}))