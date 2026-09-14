import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        self.current_channel = None

    def start_consuming(self, on_message_callback):
        channel = self.connection.channel()
        self.current_channel = channel

        def callback_wrapper(ch, method, props, body):
            # on message callback es así:
            # message - El valor tal y como lo recibe el método send de esta clase.
            # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
            # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            return on_message_callback(body, ack, nack)

        channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback_wrapper
        )

        channel.start_consuming()

    def stop_consuming(self):
        if self.current_channel:
            self.current_channel.stop_consuming()
            self.current_channel = None

    def send(self, message):
        channel = self.connection.channel()
        channel.basic_publish("", self.queue_name, message)

    def close(self):
        if self.current_channel:
            self.current_channel.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name
        channel = self.connection.channel()
        channel.exchange_declare(exchange_name, exchange_type="direct")
        self.current_channel = None

    def start_consuming(self, on_message_callback):
        channel = self.connection.channel()
        self.current_channel = channel

        def callback_wrapper(ch, method, props, body):
            # on message callback es así:
            # message - El valor tal y como lo recibe el método send de esta clase.
            # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
            # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            return on_message_callback(body, ack, nack)

        result = channel.queue_declare(queue="")
        queue_name = result.method.queue
        for routing_key in self.routing_keys:
            channel.queue_bind(
                exchange=self.exchange_name, queue=queue_name, routing_key=routing_key
            )
        channel.basic_consume(queue=queue_name, on_message_callback=callback_wrapper)

        channel.start_consuming()

    def stop_consuming(self):
        if self.current_channel:
            self.current_channel.stop_consuming()
            self.current_channel = None

    def send(self, message):
        channel = self.connection.channel()
        for routing_key in self.routing_keys:
            channel.basic_publish(self.exchange_name, routing_key, message)

    def close(self):
        if self.current_channel:
            self.current_channel.close()
