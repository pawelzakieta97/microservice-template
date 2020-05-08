import threading

# This is necessary even though it says it is not (evaluating date)
import datetime

import pika
import callme
import logging_service.log_message

class Service:
    """
    A base class to all python-based microservices. It implements an RPC server
    for calls from API gateway and handles data integration events.
    """
    def __init__(self, service_name, url='localhost', table_names=None,
                 owned_tables=None, db_connector=None, console_debug=True):
        """
        :param service_name:
        :param url:
        :param table_names:
        :param owned_tables:
        :param db_connector:
        :param console_debug:
        """
        self.service_name = service_name
        self.table_names = table_names
        self.db_con = db_connector
        if owned_tables is not None:
            self.owned_tables = owned_tables
        else:
            self.owned_tables = []

        # Opening connection with RabbotMQ for events handling
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=url))
        self.channel = connection.channel()
        self.console_debug = console_debug
        # Subscribig to events that concern tables that are not owned by this
        # service. Assuming that only an owner can edit a table and there is
        # only one owner per table, owner does not have to listen to events.
        # Listening would cause the owner to react to events that it has fired
        # itself
        if table_names is not None:
            for table_name in table_names:
                self.channel.exchange_declare(exchange=table_name,
                                              exchange_type='fanout')
                if table_name not in self.owned_tables:
                    self._subscribe_to_table(table_name)

        # initiate RPC server
        self.server = callme.Server(server_id=service_name, amqp_host=url,
                                    threaded=True)
        self.registered_tasks = []

        # initiating log exchange in case it does not yet exist
        self.channel.exchange_declare(exchange='log',
                                      exchange_type='fanout')

    def register_task(self, method, method_name):
        """Registers a task taht can be executed via RPC (this service is a
        server that other services can call to execute the function)
        :param method:
        :param method_name:
        :return:
        """
        self.server.register_function(method, method_name)
        self.registered_tasks.append(method_name)

    def _subscribe_to_table(self, table_name):
        """Subscribes the service to exchange that informs about changes in
        this table. Adjusts the local table according to the update event
        content (create/update/delete/drop).

        :param table_name:
        :return:

        TODO:
            Right now, probably, all the events are handled in a single thread.
            This means that a single instance of this service can handle only
            one database update at a time. This should be fixed by invoking
            callback function in a separate thread
        """
        queue = self.channel.queue_declare(queue=self.service_name+table_name)

        self.channel.queue_bind(exchange=table_name,
                                queue=queue.method.queue)

        def callback(ch, method, properties, body):
            """
            this method will be called every time there is an update in table
            `table_name`
            """
            data = eval(body)

            if 'method' in data.keys():

                method = data.pop('method')
                self.log(topic=f'Table {table_name} updated with {method.upper()}.',
                         content=f'Event data:{data}')
                if method == 'create':
                    self.create_record(table_name, data, force=True)
                if method == 'update':
                    self.update_record(table_name, data, force=True)
                if method == 'delete':
                    self.delete_record(table_name, data, force=True)
                if method == 'clear':
                    self.clear_table(table_name, force=True)
            pass

        self.channel.basic_consume(queue=queue.method.queue, auto_ack=True,
                                   on_message_callback=callback)

    def log(self, topic, content=None, type='log', debug=True, author=None):
        if author is None:
            author = self.service_name
        msg = logging_service.log_message.LogMessage(author=author, topic=topic, content=content, type=type, debug=debug)
        if self.console_debug:
            print(str(msg))
        self.channel.basic_publish(exchange='log', routing_key='',
                                   body=str(msg))

    def create_record(self, table_name, data, force=False):
        """Adds a record to a table

        Directly modifying a table is allowed only if the service owns the
        table. This check can be omitted by `force` flag

        :param table_name:
        :param data:
        :param force: if True, the record will be created even in tables that
        the service does not own. It should not be used in production, but
        allows for initiating the database in debug stage
        :return:
        """
        self.log(f'Adding record to table {table_name}')
        if not force and table_name not in self.owned_tables:
            raise ValueError(f'This service cant directly modify table'
                             f'{table_name} because it does not own the table.'
                             f'Call this method via RPC on a service that owns'
                             f'the table')
        if table_name in self.owned_tables:
            created, id = self.db_con.create(table_name, data)
            if created:
                # if record has been successfully created, fire an event to
                # inform other microservices of new record
                data['method'] = 'create'
                data['_id']=id
                self.channel.basic_publish(exchange=table_name, routing_key='',
                                           body=str(data))
        else:
            created, id = self.db_con.create(table_name, data)
        return created, id


    def update_record(self, table_name, data, force=False):
        if not force and table_name not in self.owned_tables:
            raise ValueError('This service cant directly modify a table that '
                             'it does not own. Call this method via RPC on a '
                             'service that owns the table')
        if table_name in self.owned_tables:
            updated = self.db_con.update(table_name, data)
            if updated:
                data['method'] = 'update'
                self.channel.basic_publish(exchange=table_name, routing_key='',
                                           body=str(data))
        else:
            updated = self.db_con.update(table_name, data)
        return updated

    def delete_record(self, table_name, data, force=False):
        if not force and table_name not in self.owned_tables:
            raise ValueError('This service cant directly modify a table that '
                             'it does not own. Call this method via RPC on a '
                             'service that owns the table')
        if table_name in self.owned_tables:
            deleted = self.db_con.delete(table_name, data)
            if deleted:
                data['method'] = 'delete'
                self.channel.basic_publish(exchange=table_name, routing_key='',
                                           body=str(data))
        else:
            deleted = self.db_con.delete(table_name, data)
        return deleted

    def clear_table(self, table_name, force=False):
        if not force and table_name not in self.owned_tables:
            raise ValueError('This service cant directly modify a table that '
                             'it does not own. Call this method via RPC on a '
                             'service that owns the table')
        if table_name in self.owned_tables:
            cleared = self.db_con.clear(table_name)
            if cleared:
                data = {'method': 'clear'}
                self.channel.basic_publish(exchange=table_name, routing_key='',
                                           body=str(data))
        else:
            cleared = self.db_con.clear(table_name)
        return cleared

    def init_table(self, table_name, data, force=False):
        for row in data:
            self.create_record(table_name, row, force)

    def run(self):
        """starts RPC server in a separate thread and starts listening to events
        """
        t = threading.Thread(target=self.server.start, daemon=True)
        t.start()
        self.channel.start_consuming()
