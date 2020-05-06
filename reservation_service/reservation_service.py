import service
from database_connector import sql_alchemy_connector
from sqlalchemy import Column, String, Integer, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
import datetime


class ReservationService(service.Service):
    def __init__(self, name='reservation_service'):
        super().__init__(name, table_names=['restaurants', 'reservations'],
                         owned_tables=['reservations'])

        # Defining the database
        base = declarative_base()
        class Restaurant(base):
            __tablename__ = 'restaurants'
            __table_args__ = {'schema': 'reservation_microservice'}
            _id = Column(Integer, primary_key=True)
            name = Column(String)
            address = Column(String)

        class Reservation(base):
            __tablename__ = 'reservations'
            __table_args__ = {"schema": "reservation_microservice"}
            _id = Column(Integer, primary_key=True)
            restaurant_id = Column(Integer, ForeignKey(
                'reservation_microservice.restaurants._id'))
            email = Column(String)
            date = Column(Date)
            time = Column(String)
            guests = Column(Integer)

        # Assigning an SQLAlchemy database connector
        self.db_con = sql_alchemy_connector.SQLAlchemyConnector([Restaurant, Reservation],
                                                                url='localhost', db_name='postgres',
                                                                username='reservation_service', password='password')

        # Creating the tables if they do not exist
        base.metadata.create_all(self.db_con.db)

        # defining the methods that this service can execute as an RPC server
        self.register_task(self.create_reservation, 'create_reservation')
        self.register_task(self.get_reservation_by_id, 'get_reservation_by_id')

    def create_reservation(self, data):
        self.log('create reservation function called')
        created, id = self.create_record('reservations', data)
        return created, id

    def get_reservation_by_id(self, id):
        return self.db_con.select('reservations', filter={'_id': id})


if __name__ == '__main__':

    # Generating initial restaurant table data
    restaurant_names = ['kfc', 'burgerking', 'subway', 'mcdonalds']
    addresses = [f'{name} street' for name in restaurant_names]
    restaurant_ids = [i for i in range(len(restaurant_names))]
    restaurants_data = [{'_id': id, 'name': name, 'address': address} for id, name, address in zip(restaurant_ids, restaurant_names, addresses)]

    #generating initial reservations darta
    user_names = ['tomek', 'robert', 'justyna', 'anton', 'khanh', 'pawel']
    emails = [f'{name}@gmail.com' for name in user_names]
    rest_ids = [i%len(restaurant_names) for i in range(len(user_names))]
    reserv_ids = [i for i in range(len(user_names))]
    reservations_data = [{'_id': reserv_id, 'email': email, 'restaurant_id': rest_id,'date': datetime.date.today(), 'time': 'breakfast'}
                         for reserv_id, email, rest_id in zip(reserv_ids, emails, rest_ids)]
    # reservations_data = [
    #     {'email': email, 'restaurant_id': rest_id,
    #      'date': datetime.date.today(), 'time': 'breakfast'}
    #     for reserv_id, email, rest_id in zip(reserv_ids, emails, rest_ids)]

    # creating the service instance
    service = ReservationService()
    # comment this line to use actual database
    # service.db_con = db_connector.DBConnectorMock()

    # force-cleaning
    service.clear_table('reservations', force=True)
    service.clear_table('restaurants', force=True)
    # Initiating tables
    service.init_table('restaurants', restaurants_data, force=True)
    service.init_table('reservations', reservations_data)

    reservation_json = {'email': 'new_email@gmail.com',
                        'date': datetime.date.today(), 'time': 'dinner'}
    # service.create_record('reservations', {'_id': 7, 'email': 'email@email.com', 'restaurant_id': 1,'date': datetime.date.today(), 'time': 'breakfast'})
    service.create_reservation(reservation_json)
    service.run()
