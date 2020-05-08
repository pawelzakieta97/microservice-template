import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import service

class LogService(service.Service):
    def __init__(self, filename='log.txt'):
        super().__init__('Logger')
        queue = self.channel.queue_declare(queue='logger')
        self.channel.exchange_declare(exchange='log',
                                      exchange_type='fanout')
        self.channel.queue_bind(exchange='log',
                                queue=queue.method.queue)
        self.filename = filename

        def callback(ch, method, properties, body):
            """
            this method will be called every time there is an update in table
            `table_name`
            """
            body_str = str(body)
            body_str = body_str[1:]
            body = eval(body_str)
            # body = re.sub('\\t', '\t', body)
            print(body)
            file = open(self.filename, 'a')
            file.write(body+'\n')
            file.close()

        self.channel.basic_consume(queue=queue.method.queue, auto_ack=True,
                                   on_message_callback=callback)

if __name__ == '__main__':
    service = LogService()
    service.run()