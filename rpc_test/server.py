import pika


class RpcServer:
    def __init__(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = connection.channel()
        print(" [rpc] Awaiting RPC requests")

    def register_function(self, function, routing_key='rpc_queue'):
        self.channel.queue_declare(queue=routing_key)
        self.channel.basic_qos(prefetch_count=1)
        def callback(ch, method, props, body):
            print(f'[rpc] RPC server gor request for {routing_key} with parameter {body}')
            response = function(body)
            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 props.correlation_id),
                             body=str(response))
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=routing_key,
                                   on_message_callback=callback)


if __name__ == '__main__':
    print('server main')
    def fib(data):
        n = int(data)
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return fib(n - 1) + fib(n - 2)

    def strlen(data):
        return len(data)

    server = RpcServer()
    server.register_function(fib, 'rpc_queue')
    server.register_function(strlen, 'strlen_queue')
    server.channel.start_consuming()