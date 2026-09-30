import os
import logging
import threading
import signal
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = self._connect_aggregators()
        
        self.control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
        )

        
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )
        self.control_output_exchanges = self._connect_aggregators()

        
        self.amounts_by_client = {}   
        self.pending_by_client = {}   
        self.total_by_client = {}     
        self.lock = threading.Lock()
        self.control_thread = None

    def _connect_aggregators(self):
        exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
            )
        return exchanges

    def _aggregator_index(self, fruit):
        
        return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

    def _take_pending(self, client_id):
        fruits = self.amounts_by_client.get(client_id, {})
        count = self.pending_by_client.get(client_id, 0)
        self.amounts_by_client[client_id] = {}
        self.pending_by_client[client_id] = 0
        return fruits, count

    def _flush(self, client_id, total, fruits, count, output_exchanges):
        if count == 0:
            return
        shards = [[] for _ in range(AGGREGATION_AMOUNT)]
        for item in fruits.values():
            shards[self._aggregator_index(item.fruit)].append(
                [item.fruit, item.amount]
            )
        for shard, exchange in zip(shards, output_exchanges):
            exchange.send(
                message_protocol.internal.serialize(
                    [client_id, "DATA", total, count, shard]
                )
            )

    # ---------------- thread de datos ----------------

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        with self.lock:
            client_fruits = self.amounts_by_client.setdefault(client_id, {})
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.pending_by_client[client_id] = (
                self.pending_by_client.get(client_id, 0) + 1
            )
            
            total = self.total_by_client.get(client_id)
            pending = self._take_pending(client_id) if total is not None else None

        if pending is not None:
            fruits, count = pending
            self._flush(client_id, total, fruits, count, self.data_output_exchanges)

    def _notify_eof(self, fields):
        
        logging.info("Notifying EOF to all Sum instances")
        self.control_publisher.send(message_protocol.internal.serialize(fields))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) >= 2:
            msg_type = fields[1]
            if msg_type == "DATA" and len(fields) == 4:
                self._process_data(fields[0], fields[2], fields[3])
            elif msg_type == "EOF" and len(fields) == 3:
                self._notify_eof(fields)
        ack()

    # ---------------- thread de control ----------------

    def _process_eof(self, client_id, total):
        logging.info("EOF notification received, flushing")
        with self.lock:
            self.total_by_client[client_id] = total
            fruits, count = self._take_pending(client_id)
        self._flush(client_id, total, fruits, count, self.control_output_exchanges)

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3 and fields[1] == "EOF":
            self._process_eof(fields[0], fields[2])
        ack()

    def _run_control(self):
        try:
            self.control_exchange.start_consuming(self.process_control_message)
        except Exception:
            logging.exception("Control thread failed")
            self.input_queue.stop_consuming()

    # ---------------- ciclo de vida ----------------

    def handle_signal(self, signum, frame):
        logging.info("Signal received, shutting down")
        self.input_queue.stop_consuming()
        self.control_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)

        self.control_thread = threading.Thread(target=self._run_control)
        self.control_thread.start()
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.control_exchange.stop_consuming()
            self.control_thread.join()
            self.stop()

    def stop(self):
        resources = [
            self.input_queue,
            self.control_publisher,
            self.control_exchange,
            *self.data_output_exchanges,
            *self.control_output_exchanges,
        ]
        for resource in resources:
            try:
                resource.close()
            except Exception:
                logging.exception("Error closing middleware resource")


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    try:
        sum_filter.start()
    except Exception:
        logging.exception("SumFilter failed")
        return 1

    return 0


if __name__ == "__main__":
    main()