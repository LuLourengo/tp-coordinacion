import contextlib
import pika
import pika.exceptions

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

PREFETCH_COUNT = 1


"""
Use un contextmanager (_translate_pika_errors) para centralizar el manejo de excepciones de Pika.
Para no repetir bloques try/except idénticos en cada método de publicación, consumo y declaración,
 tal como se corrigio en el tp 1.

"""

@contextlib.contextmanager
def _translate_pika_errors(action):
    try:
        yield
    except pika.exceptions.AMQPConnectionError as err:
        raise MessageMiddlewareDisconnectedError(
            f"Conexion perdida al {action}: {err}"
        ) from err
    except pika.exceptions.AMQPError as err:
        raise MessageMiddlewareMessageError(
            f"Error al {action}: {err}"
        ) from err


class MessageAcknowledger:
    def __init__(self, channel, delivery_tag):
        self.channel = channel
        self.delivery_tag = delivery_tag

    def ack(self):
        with _translate_pika_errors("hacer ack del mensaje"):
            self.channel.basic_ack(delivery_tag=self.delivery_tag)

    def nack(self):
        with _translate_pika_errors("hacer nack del mensaje"):
            self.channel.basic_nack(delivery_tag=self.delivery_tag, requeue=True)


"""
Cree la clase _RabbitMQMiddleware, como correccion del tp1 por repeticion de codigo.

Tanto MessageMiddlewareQueueRabbitMQ como MessageMiddlewareExchangeRabbitMQ compartían implementación:
establecer la BlockingConnection, abrir el canal, setear el basic_qos(prefetch_count=1), 
implementar la detención thread-safe con add_callback_threadsafe y manejar los acks en el consumo.
Extraje la lógica compartida en esta clase y las clases hijas ahora solo se encargan de lo que las diferencia:
 cómo se declaran las colas en RabbitMQ y si usan o no un exchange con routing keys.

"""


class _RabbitMQMiddleware:

    def __init__(self, host):
        self.host = host
        self.connection = None
        self.channel = None
        self.is_consuming = False
        self._user_callback = None
        self._consume_queues = []
        self._description = ""

        with _translate_pika_errors(f"conectar con el broker en {host}"):
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    def _publish(self, exchange, routing_keys, message):
        with _translate_pika_errors(f"enviar a {self._description}"):
            for routing_key in routing_keys:
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=message,
                )

    def _on_message_received(self, channel, method, properties, body):
        acknowledger = MessageAcknowledger(channel, method.delivery_tag)
        self._user_callback(body, acknowledger.ack, acknowledger.nack)

    def start_consuming(self, on_message_callback):
        self._user_callback = on_message_callback
        try:
            with _translate_pika_errors(f"consumir de {self._description}"):
                self.is_consuming = True
                for queue_name in self._consume_queues:
                    self.channel.basic_consume(
                        queue=queue_name,
                        on_message_callback=self._on_message_received,
                        auto_ack=False,
                    )
                self.channel.start_consuming()
        finally:
            self.is_consuming = False

    def _stop_consuming_callback(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        finally:
            self.is_consuming = False

    def stop_consuming(self):
        if not self.is_consuming:
            return

        with _translate_pika_errors("detener el consumo"):
            self.connection.add_callback_threadsafe(self._stop_consuming_callback)

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPConnectionError:
            pass
        except pika.exceptions.AMQPError as err:
            raise MessageMiddlewareCloseError(
                f"Error al cerrar {self._description}: {err}"
            ) from err


class MessageMiddlewareQueueRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name
        self._description = f"la cola '{queue_name}'"
        self._consume_queues = [queue_name]

        with _translate_pika_errors(f"declarar {self._description}"):
            self.channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        self._publish("", [self.queue_name], message)


class MessageMiddlewareExchangeRabbitMQ(
    _RabbitMQMiddleware, MessageMiddlewareExchange
):

    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self.exchange_name = exchange_name
        self.routing_keys = list(routing_keys or [])
        self._description = f"el exchange '{exchange_name}'"

        with _translate_pika_errors(f"declarar {self._description}"):
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
                self._consume_queues.append(queue_name)

    def send(self, message):
        self._publish(self.exchange_name, self.routing_keys, message)