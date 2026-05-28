import os
import logging
import signal

from common import middleware, message_protocol

MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_QUEUE = os.environ.get("INPUT_QUEUE", "client_count_shards")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "counted_transactions_5")

AGGREGATION_AMOUNT = int(os.environ.get("AGGREGATION_AMOUNT", "1"))

class PaymentMethodJoiner:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        
        self.total_counts_by_client = {}
        self.eof_count_by_client = {}

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields["client_id"]
            partial_counts = fields["counts"]

            logging.info(f"Recibido conteo parcial del aggregator para {client_id[:8]}")

            if client_id not in self.total_counts_by_client:
                self.total_counts_by_client[client_id] = {}
                self.eof_count_by_client[client_id] = 0

            for payment_format, count in partial_counts.items():
                current_count = self.total_counts_by_client[client_id].get(payment_format, 0)
                self.total_counts_by_client[client_id][payment_format] = current_count + count

            self.eof_count_by_client[client_id] += 1

            if self.eof_count_by_client[client_id] == AGGREGATION_AMOUNT:
                logging.info(f"Recibidas todas las partes para {client_id[:8]}. Enviando resultado final al Gateway.")

                final_counts = self.total_counts_by_client[client_id]

                final_result = {
                    "client_id": client_id,
                    "counts": final_counts
                }

                self.output_queue.send(message_protocol.internal.serialize(final_result))

                del self.total_counts_by_client[client_id]
                del self.eof_count_by_client[client_id]

            ack()
        except Exception as e:
            logging.error(f"Error procesando mensaje en Joiner: {e}")
            nack()

    def handle_sigterm(self, signum, frame):
        logging.info("SIGTERM recibido. Deteniendo consumo...")
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        logging.info("Joiner iniciado y escuchando...")
        
        self.input_queue.start_consuming(self.process_message)
        
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    joiner = PaymentMethodJoiner()
    joiner.start()
    return 0

if __name__ == "__main__":
    main()