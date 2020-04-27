import pika
import uuid

class RpcClient(object):

    def __init__(self):
        self.connection_client = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection_client.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, param=None, routing_key='rpc_queue'):
        print(f'[rpc] calling remote procedure {routing_key} with parameter {param}')
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(param))
        while self.response is None:
            self.connection_client.process_data_events()
        print(f'[rpc] remote procedure returned value of {self.response}')
        return self.response


if __name__ == '__main__':
    fibonacci_rpc = RpcClient()

    print(" [x] Requesting fib(30)")
    response = fibonacci_rpc.call(30)
    print(" [.] Got %r" % response)
    response = fibonacci_rpc.call('asd123', 'strlen_queue')
    print(" [.] Got %r" % response)