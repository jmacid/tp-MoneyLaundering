import os
import logging
import signal
from common import middleware, message_protocol

ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_EXCHANGE = os.environ.get("INPUT_EXCHANGE", "client_average_shards_3")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "calculated_averages_3")
AGGREGATION_AMOUNT = int(os.environ.get("AGGREGATION_AMOUNT", "1"))

class AverageJoiner:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, INPUT_EXCHANGE, [f"{INPUT_EXCHANGE}_{ID}"])
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.total_averages_by_client = {}
        self.eof_count_by_client = {}

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields["client_id"]
            partial_counts = fields["counts"]

            if client_id not in self.total_averages_by_client:
                self.total_averages_by_client[client_id] = {}
                self.eof_count_by_client[client_id] = 0

            for payment_format, data in partial_counts.items():
                if payment_format not in self.total_averages_by_client[client_id]:
                    self.total_averages_by_client[client_id][payment_format] = {"sum": 0.0, "count": 0.0}
                
                self.total_averages_by_client[client_id][payment_format]["sum"] += data["sum"]
                self.total_averages_by_client[client_id][payment_format]["count"] += data["count"]

            self.eof_count_by_client[client_id] += 1

            if self.eof_count_by_client[client_id] == AGGREGATION_AMOUNT:
                final_result = {
                    "client_id": client_id,
                    "counts": self.total_averages_by_client[client_id]
                }
                self.output_queue.send(message_protocol.internal.serialize(final_result))
                del self.total_averages_by_client[client_id]
                del self.eof_count_by_client[client_id]

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
        self.output_queue.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    AverageJoiner().start()