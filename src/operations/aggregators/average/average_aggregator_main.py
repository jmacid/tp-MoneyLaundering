import os
import logging
import signal
import hashlib
from common import middleware, message_protocol

ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_EXCHANGE = os.environ.get("INPUT_EXCHANGE", "local_average_3")
OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "client_average_shards_3")
COUNTER_AMOUNT = int(os.environ.get("COUNTER_AMOUNT", "1"))
JOINERS_COUNT = int(os.environ.get("JOINERS_COUNT", "1"))

class AverageAggregator:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, INPUT_EXCHANGE, [f"{INPUT_EXCHANGE}_{ID}"])
        self.data_output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, OUTPUT_EXCHANGE, [f"{OUTPUT_EXCHANGE}_{i}"])
            for i in range(JOINERS_COUNT)
        ]
        self.averages_by_client = {}
        self.eof_count_by_client = {}

    def _process_data(self, client_id, payment_format, sum_val, count_val):
        if client_id not in self.averages_by_client:
            self.averages_by_client[client_id] = {}
        client_avg = self.averages_by_client[client_id]
        if payment_format not in client_avg:
            client_avg[payment_format] = {"sum": 0.0, "count": 0.0}
            
        client_avg[payment_format]["sum"] += sum_val
        client_avg[payment_format]["count"] += count_val

    def _process_eof(self, client_id):
        self.eof_count_by_client[client_id] = self.eof_count_by_client.get(client_id, 0) + 1
        if self.eof_count_by_client[client_id] < COUNTER_AMOUNT:
            return

        counts = self.averages_by_client.get(client_id, {})
        result_message = {"client_id": client_id, "counts": counts}
        
        hash_val = int(hashlib.md5(client_id.encode('utf-8')).hexdigest(), 16)
        joiner_index = hash_val % JOINERS_COUNT

        self.data_output_exchanges[joiner_index].send(message_protocol.internal.serialize(result_message))
        self.averages_by_client.pop(client_id, None)
        self.eof_count_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if isinstance(fields, dict) and "client_id" in fields and "payment_format" in fields:
                self._process_data(fields["client_id"], fields["payment_format"], fields["sum"], fields["count"])
            elif isinstance(fields, list) and len(fields) == 1:
                self._process_eof(fields[0])
            ack()
        except Exception as e:
            logging.error(f"Error: {e}")
            nack()

    def handle_sigterm(self, signum, frame):
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_exchange.start_consuming(self.process_message)
        self.input_exchange.close()
        for exchange in self.data_output_exchanges: exchange.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    AverageAggregator().start()