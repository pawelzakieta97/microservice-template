import callme
import time

def add(a, b):
    time.sleep(5)
    return a + b


server = callme.Server(server_id='fooserver',
                       amqp_host='localhost', threaded=True)

server.register_function(add, 'add')
server.start()