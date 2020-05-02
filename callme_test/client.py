import callme

proxy = callme.Proxy(server_id='fooserver', amqp_host='localhost',)
print(proxy.use_server('fooserver').add(1, 2))