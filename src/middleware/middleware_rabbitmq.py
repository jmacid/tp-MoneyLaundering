import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.ch = connection.channel()
        self.queue_name = queue_name
        self.ch.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})

    def close(self):
        self.ch.close()

    def send(self, message):
        self.ch.basic_publish(exchange='',
                      routing_key=self.queue_name,
                      body=message)

    def start_consuming(self, callback):
        def pika_callbackdef(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            callback(body, ack, nack)

        self.ch.basic_consume(queue=self.queue_name,
                        auto_ack=False,
                        on_message_callback=pika_callbackdef)

        self.ch.start_consuming()

    def stop_consuming(self):
        self.ch.stop_consuming()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.ch = connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.queue_name = None

        self.ch.exchange_declare(exchange=exchange_name, exchange_type='direct')

    def close(self):
        self.ch.close()

    def send(self, message):
        r_key = self.routing_keys[0] if self.routing_keys else ''
        self.ch.basic_publish(exchange=self.exchange_name, routing_key=r_key, body=message)

    def start_consuming(self, callback):
        result = self.ch.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue

        for routing_key in self.routing_keys:
            self.ch.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)

        def pika_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            callback(body, ack, nack)

        self.ch.basic_consume(
            queue=self.queue_name,
            on_message_callback=pika_callback,
            auto_ack=False
        )

        self.ch.start_consuming()

    def stop_consuming(self):
        self.ch.stop_consuming()