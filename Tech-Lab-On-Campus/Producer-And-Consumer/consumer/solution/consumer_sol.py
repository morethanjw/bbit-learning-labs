import pika
import os
import consumer_interface

class mqConsumer(consumer_interface.mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name, channel=None, connection=None):
        super().__init__(binding_key, exchange_name, queue_name)
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()
        self.startConsuming()

    def onMessageCallback(self, connection, body):
        print(body)
        connection.close()

    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name)
        channel.queue_bind(queue= self.queue_name, exchange=self.exchange_name, routing_key=self.binding_key)
        channel.basic_consume(self.queue_name, self.onMessageCallback, auto_ack=False)
        self.connection = connection
        self.channel = channel

    def startConsuming(self):
        self.channel.start_consuming()

    def delIt(self):
        self.channel.close()
        self.connection.close()