import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


MSG_TYPE_DATA = "DATA"
MSG_TYPE_EOF = "EOF"

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruits_by_client = {}   
        self.counted_by_client = {}  
        self.total_by_client = {}  

    def _process_data(self, client_id, total, count, fruits):
        logging.info("Processing data message")
        client_fruits = self.fruits_by_client.setdefault(client_id, {})
        for fruit, amount in fruits:
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

        self.counted_by_client[client_id] = (
            self.counted_by_client.get(client_id, 0) + count
        )
        self.total_by_client[client_id] = total

        
        if self.counted_by_client[client_id] == total:
            self._process_eof(client_id)

    def _process_eof(self, client_id):
        logging.info("All data received, sending partial top")
        client_fruits = self.fruits_by_client.pop(client_id, {})
        del self.counted_by_client[client_id]
        del self.total_by_client[client_id]

        
        sorted_fruits = sorted(client_fruits.values(), reverse=True)

        fruit_top = []
        for item in sorted_fruits[:TOP_SIZE]:
            fruit_top.append([item.fruit, item.amount])

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, MSG_TYPE_DATA, fruit_top])
        )
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, MSG_TYPE_EOF])
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 5 and fields[1] == MSG_TYPE_DATA:
            self._process_data(fields[0], fields[2], fields[3], fields[4])
        ack()

    def handle_signal(self, signum, frame):
        logging.info("Signal received, shutting down")
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            self.stop()

    def stop(self):
        for resource in (self.input_exchange, self.output_queue):
            try:
                resource.close()
            except Exception:
                logging.exception("Error closing middleware resource")


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    try:
        aggregation_filter.start()
    except Exception:
        logging.exception("AggregationFilter failed")
        return 1

    return 0


if __name__ == "__main__":
    main()