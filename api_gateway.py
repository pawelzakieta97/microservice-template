import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# channel.queue_declare(queue='hello')
channel.exchange_declare(exchange='new_reservation', exchange_type='fanout')

reservation_json = {'user_id': '123123', 'table_id': '123',
                         'date': '01.06.2020', 'time': 'dinner'}

print(str(reservation_json))

channel.basic_publish(exchange='new_reservation', routing_key='', body=str(reservation_json))
# print("[x] new reservation")
# connection.close()