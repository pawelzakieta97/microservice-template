import pika
import rpc_test.server

class ReservationService(rpc_test.server.RpcServer):
    def __init__(self):
        super().__init__()
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='new_reservation',
                                 exchange_type='fanout')
        queue = self.channel.queue_declare(queue='reservation_queue',
                                      auto_delete=True)
        self.channel.queue_bind(exchange='new_reservation', queue=queue.method.queue)
        self.channel.basic_consume(queue=queue.method.queue, auto_ack=True,
                          on_message_callback=self.new_reservation_callback)

        self.register_function(self.get_table_details, 'get_table_details')

    def get_table_details(self, table_id):
        return f'mock details for table {table_id}'

    def new_reservation_callback(self, ch, method, properties, body):
        print(f"[x] Received {body}\n. This service should now put the reservation in the database")

    def run(self):
        self.channel.start_consuming()

if __name__ == '__main__':
    service = ReservationService()
    service.run()