import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

PREFETCH_COUNT = 1


class MessageAcknowledger:
    def __init__(self, channel, delivery_tag):
        self.channel = channel
        self.delivery_tag = delivery_tag

    def ack(self):
        self.channel.basic_ack(delivery_tag=self.delivery_tag)

    def nack(self):
        self.channel.basic_nack(delivery_tag=self.delivery_tag, requeue=True)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer_tag = None
        self.is_consuming = False
        self._user_callback = None

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self.channel.queue_declare(queue=self.queue_name)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as err:
            raise MessageMiddlewareDisconnectedError(
                f"Fallo de conexion con el broker en {self.host}: {err}"
            )
        except (pika.exceptions.AMQPChannelError, Exception) as err:
            raise MessageMiddlewareMessageError(
                f"Error al declarar la cola '{self.queue_name}': {err}"
            )

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError) as err:
            raise MessageMiddlewareDisconnectedError(
                f"Fallo de conexion al enviar a la cola '{self.queue_name}': {err}"
            )
        except (pika.exceptions.AMQPChannelError, pika.exceptions.ChannelClosedByBroker) as err:
            raise MessageMiddlewareMessageError(
                f"Canal cerrado al enviar a la cola '{self.queue_name}': {err}"
            )
        except Exception as err:
            raise MessageMiddlewareMessageError(
                f"Error al enviar mensaje a la cola '{self.queue_name}': {err}"
            )

    def _on_message_received(self, channel, method, properties, body):
        handler = MessageAcknowledger(channel, method.delivery_tag)
        self._user_callback(body, handler.ack, handler.nack)

    def start_consuming(self, on_message_callback):
        self._user_callback = on_message_callback

        try:
            self.is_consuming = True
            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._on_message_received,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError) as err:
            self.is_consuming = False
            raise MessageMiddlewareDisconnectedError(
                f"Conexion perdida durante el consumo en '{self.queue_name}': {err}"
            )
        except (pika.exceptions.AMQPChannelError, pika.exceptions.ChannelClosedByBroker) as err:
            self.is_consuming = False
            raise MessageMiddlewareMessageError(
                f"Error de canal durante el consumo en '{self.queue_name}': {err}"
            )
        except Exception as err:
            self.is_consuming = False
            raise err

    def stop_consuming(self):
        if not self.is_consuming:
            return

        def _stop():
    
            try:
                if self.channel.is_open:
                    self.channel.stop_consuming()
            finally:
                self.is_consuming = False

        try:
            
            self.connection.add_callback_threadsafe(_stop)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError) as err:
            raise MessageMiddlewareDisconnectedError(
                f"Fallo de conexion al detener consumo: {err}"
            )

    def close(self):
        try:
            if self.is_consuming:
                self.stop_consuming()

            if self.channel and self.channel.is_open:
                self.channel.close()

            if self.connection and self.connection.is_open:
                self.connection.close()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            pass
        except Exception as err:
            raise MessageMiddlewareCloseError(
                f"Error al cerrar la cola '{self.queue_name}': {err}"
            )


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name

        if routing_keys is None:
            self.routing_keys = []
        else:
            self.routing_keys = routing_keys

        self.connection = None
        self.channel = None
        self.queue_names = []
        self.consumer_tag = None
        self.is_consuming = False
        self._user_callback = None

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type="direct",
            )
            
            for routing_key in self.routing_keys:
                queue_name = f"{self.exchange_name}_{routing_key}"
                self.channel.queue_declare(queue=queue_name)
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=queue_name,
                    routing_key=routing_key,
                )
                self.queue_names.append(queue_name)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as err:
            raise MessageMiddlewareDisconnectedError(
                f"Fallo de conexion con el broker en {self.host}: {err}"
            )
        except (pika.exceptions.AMQPChannelError, Exception) as err:
            raise MessageMiddlewareMessageError(
                f"Error al declarar el exchange '{self.exchange_name}': {err}"
            )

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError) as err:
            raise MessageMiddlewareDisconnectedError(
                f"Fallo de conexion al publicar en '{self.exchange_name}': {err}"
            )
        except (pika.exceptions.AMQPChannelError, pika.exceptions.ChannelClosedByBroker) as err:
            raise MessageMiddlewareMessageError(
                f"Canal cerrado al publicar en '{self.exchange_name}': {err}"
            )
        except Exception as err:
            raise MessageMiddlewareMessageError(
                f"Error al publicar en '{self.exchange_name}': {err}"
            )

    def _on_message_received(self, channel, method, properties, body):
        handler = MessageAcknowledger(channel, method.delivery_tag)
        self._user_callback(body, handler.ack, handler.nack)

    def start_consuming(self, on_message_callback):
        self._user_callback = on_message_callback

        try:
            self.is_consuming = True
            for queue_name in self.queue_names:
                self.consumer_tag = self.channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self._on_message_received,
                    auto_ack=False,
                )
            self.channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError) as err:
            self.is_consuming = False
            raise MessageMiddlewareDisconnectedError(
                f"Conexion perdida consumiendo exchange '{self.exchange_name}': {err}"
            )
        except (pika.exceptions.AMQPChannelError, pika.exceptions.ChannelClosedByBroker) as err:
            self.is_consuming = False
            raise MessageMiddlewareMessageError(
                f"Error de canal consumiendo exchange '{self.exchange_name}': {err}"
            )
        except Exception as err:
            self.is_consuming = False
            raise err

    def stop_consuming(self):
        if not self.is_consuming:
            return

        def _stop():
            
            try:
                if self.channel.is_open:
                    self.channel.stop_consuming()
            finally:
                self.is_consuming = False

        try:
            
            self.connection.add_callback_threadsafe(_stop)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError) as err:
            raise MessageMiddlewareDisconnectedError(
                f"Fallo de conexion al detener consumo del exchange: {err}"
            )

    def close(self):
        try:
            if self.is_consuming:
                self.stop_consuming()

            if self.channel and self.channel.is_open:
                self.channel.close()

            if self.connection and self.connection.is_open:
                self.connection.close()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            pass
        except Exception as err:
            raise MessageMiddlewareCloseError(
                f"Error al cerrar exchange '{self.exchange_name}': {err}"
            )