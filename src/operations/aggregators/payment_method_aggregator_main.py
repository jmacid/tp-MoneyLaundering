import os
import logging
import signal

from common import middleware, message_protocol

ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_EXCHANGE = os.environ.get("INPUT_EXCHANGE", "local_count_transactions")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "client_count_shards")

COUNTER_AMOUNT = int(os.environ.get("COUNTER_AMOUNT", "1")) 

class PaymentMethodAggregator:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"{INPUT_EXCHANGE}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        
        self.counts_by_client = {}

        self.eof_count_by_client = {}

    def _process_data(self, client_id, payment_format, count):
        logging.info(f"Procesando conteo para {client_id[:8]}: {payment_format} -> +{count}")

        if client_id not in self.counts_by_client:
            self.counts_by_client[client_id] = {}

        client_counts = self.counts_by_client[client_id]
        client_counts[payment_format] = client_counts.get(payment_format, 0) + count

    def _process_eof(self, client_id):
        logging.info(f"EOF recibido para el cliente {client_id[:8]}")

        if client_id not in self.eof_count_by_client:
            self.eof_count_by_client[client_id] = 0
        self.eof_count_by_client[client_id] += 1

        if self.eof_count_by_client[client_id] < COUNTER_AMOUNT:
            return

        logging.info(f"Todos los EOF recibidos para {client_id[:8]}. Consolidando y emitiendo...")
        
        counts = self.counts_by_client.get(client_id, {})
        
        result_message = {
            "client_id": client_id,
            "counts": counts
        }
        
        self.output_queue.send(message_protocol.internal.serialize(result_message))
        
        if client_id in self.counts_by_client:
            del self.counts_by_client[client_id]
        del self.eof_count_by_client[client_id]

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            
            if isinstance(fields, dict) and "client_id" in fields and "payment_format" in fields:
                self._process_data(fields["client_id"], fields["payment_format"], fields["count"])
            elif isinstance(fields, list) and len(fields) == 1:
                self._process_eof(fields[0])
            
            ack()
        except Exception as e:
            logging.error(f"Error procesando mensaje en Aggregator: {e}")
            nack()

    def handle_sigterm(self, signum, frame):
        logging.info("SIGTERM recibido. Cerrando aggregator...")
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        logging.info(f"Aggregator {ID} iniciado y escuchando...")
        
        self.input_exchange.start_consuming(self.process_message)
        
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    aggregator = PaymentMethodAggregator()
    aggregator.start()
    return 0

if __name__ == "__main__":
    main()