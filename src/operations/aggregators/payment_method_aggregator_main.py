import os
import logging
import signal
import hashlib

from common import middleware, message_protocol

ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_EXCHANGE = os.environ.get("INPUT_EXCHANGE", "local_count_transactions")
OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "client_count_shards_5")

COUNTER_AMOUNT = int(os.environ.get("COUNTER_AMOUNT", "1")) 
JOINERS_COUNT = int(os.environ.get("JOINERS_COUNT", "1"))
class PaymentMethodAggregator:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"{INPUT_EXCHANGE}_{ID}"]
        )

        self.data_output_exchanges = []
        for i in range(JOINERS_COUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE, [f"{OUTPUT_EXCHANGE}_{i}"]
            )
            self.data_output_exchanges.append(exchange)

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
        
        hash_val = int(hashlib.md5(client_id.encode('utf-8')).hexdigest(), 16)
        joiner_index = hash_val % JOINERS_COUNT

        self.data_output_exchanges[joiner_index].send(
            message_protocol.internal.serialize(result_message)
        )
        
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
        for exchange in self.data_output_exchanges:
            exchange.close()


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