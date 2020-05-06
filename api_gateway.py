import pika
import callme
import datetime

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
# channel.exchange_declare(exchange='new_reservation', exchange_type='fanout')

restaurant_json = {'method': 'create', 'name': 'restaurant123',
                   'address': 'restaurant street 123'}
reservation_json = {'email': 'new_email@gmail.com',
                    'date': datetime.date.today(), 'time': 'dinner'}
# channel.basic_publish(exchange='restaurants', routing_key='', body=str(restaurant_json))

# RPC test
proxy = callme.Proxy(server_id='reservation_service', amqp_host='localhost',)
created, id = proxy.use_server('reservation_service').create_reservation(reservation_json)
print(created)
print(id)

# print("[x] new reservation")
# connection.close()