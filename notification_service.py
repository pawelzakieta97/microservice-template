import pika
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# import pymongo
import rpc_test.client

# set up the SMTP server
# s = smtplib.SMTP('smtp.gmail.com', 587)
EMAIL_ADDR = 'cyanide_service@wp.pl'
PASSWORD = 'password'

class NotificationService(rpc_test.client.RpcClient):
    def __init__(self):

        super().__init__()
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='new_reservation',
                                 exchange_type='fanout')
        queue = self.channel.queue_declare(queue='notification_queue',
                                      auto_delete=True)
        self.channel.queue_bind(exchange='new_reservation', queue=queue.method.queue)
        self.channel.basic_consume(queue=queue.method.queue, auto_ack=True,
                              on_message_callback=self.new_reservation_callback)

        # print(self.call('123', 'get_table_details'))


    def new_reservation_callback(self, ch, methof, properties, body):
        reservation_info = eval(body)
        table_id = reservation_info['table_id']
        print(f"[x] Received {body}")
        table_details = self.call(table_id, 'get_table_details')
        print('rpc finished')
        print(f"New entry in notification database should be added."
              f"details to be added: {table_details}")

    def send_email(message):
        s = smtplib.SMTP('smtp.wp.pl', 587)
        s.starttls()
        s.login(EMAIL_ADDR, PASSWORD)
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDR
        msg['To'] = 'pawelzakieta97@gmail.com'
        msg['Subject'] = 'test message'
        msg.attach(MIMEText(message, 'plain'))
        s.send_message(msg)
        del msg

    def run(self):
        self.channel.start_consuming()


if __name__ == '__main__':
    # client = pymongo.MongoClient(
    #     "mongodb+srv://admin:admin@cluster0-jinrj.mongodb.net/test?retryWrites=true&w=majority")
    # db = client.test
    # serverStatusResult = db.command("serverStatus")
    # print(serverStatusResult)
    service = NotificationService()
    service.run()
