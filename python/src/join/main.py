import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


MSG_TYPE_DATA = "DATA"
MSG_TYPE_EOF = "EOF"

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.tops_by_client = {}
        self.eofs_by_client = {}

    def _process_data(self, client_id, partial_top):
        logging.info("Received top")
        self.tops_by_client.setdefault(client_id, []).extend(partial_top)

    def _process_eof(self, client_id):
        self.eofs_by_client[client_id] = self.eofs_by_client.get(client_id, 0) + 1

        
        if self.eofs_by_client[client_id] < AGGREGATION_AMOUNT:
            return

        del self.eofs_by_client[client_id]
        client_candidates = self.tops_by_client.pop(client_id, [])

        
        items = [
            fruit_item.FruitItem(fruit, amount)
            for fruit, amount in client_candidates
        ]
        sorted_items = sorted(items, reverse=True)

        final_top = [[item.fruit, item.amount] for item in sorted_items[:TOP_SIZE]]
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) >= 2:
            client_id = fields[0]
            msg_type = fields[1]

            if msg_type == MSG_TYPE_DATA and len(fields) == 3:
                self._process_data(client_id, fields[2])
            elif msg_type == MSG_TYPE_EOF:
                self._process_eof(client_id)
        ack()

    def handle_signal(self, signum, frame):
        logging.info("Signal received, shutting down")
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.stop()

    def stop(self):
        for resource in (self.input_queue, self.output_queue):
            try:
                resource.close()
            except Exception:
                logging.exception("Error closing middleware resource")


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    try:
        join_filter.start()
    except Exception:
        logging.exception("JoinFilter failed")
        return 1

    return 0


if __name__ == "__main__":
    main()