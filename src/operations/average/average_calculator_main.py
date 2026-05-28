import os
import logging
import threading
import hashlib
import signal

from common import middleware, message_protocol

MOM_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
INPUT_QUEUE = os.getenv("INPUT_QUEUE", "minor_transactions_5")

AGGREGATION_PREFIX = os.getenv("AGGREGATION_PREFIX", "local_count_transactions")
AGGREGATION_AMOUNT = int(os.getenv("AGGREGATION_AMOUNT", "1"))

CONTROL_EXCHANGE = os.getenv("CONTROL_EXCHANGE", "payment_counter_control_exchange")

class PaymentMethodCounter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [CONTROL_EXCHANGE]
        )

        self.control_exchange_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [CONTROL_EXCHANGE]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.count_by_client_and_payment = {}
        self.lock = threading.Lock()

    def _process_data(self, transaction):
        client_id = transaction["client_id"]
        payment_format = transaction["payment_format"]
        amount_paid = float(transaction["amount_paid"])

        logging.info(f"[_process_data]: Contando {payment_format} para cliente {client_id[:8]}")
        
        with self.lock:
            if client_id not in self.count_by_client_and_payment:
                self.count_by_client_and_payment[client_id] = {}

            client_data = self.count_by_client_and_payment[client_id]
            if payment_format not in client_data:
                client_data[payment_format] = {"count": 0, "sum": 0.0}

            client_data[payment_format]["count"] += 1
            client_data[payment_format]["sum"] += amount_paid

    def _process_eof(self, client_id):
        logging.info(f"Recibido EOF. Vaciando resultados parciales para el cliente {client_id[:8]}")

        with self.lock:
            if client_id in self.count_by_client_and_payment:
                for payment_format, data in self.count_by_client_and_payment[client_id].items():
                    hash_val = int(hashlib.md5(payment_format.encode('utf-8')).hexdigest(), 16)
                    aggregator_index = hash_val % AGGREGATION_AMOUNT

                    result = {
                        "client_id": client_id,
                        "payment_format": payment_format,
                        "sum": data["sum"],
                        "count": float(data["count"])
                    }

                    self.data_output_exchanges[aggregator_index].send(
                        message_protocol.internal.serialize(result)
                    )
                del self.count_by_client_and_payment[client_id]

        logging.info(f"Enviando señal EOF a los aggregators para {client_id[:8]}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        
        if isinstance(fields, dict) and "client_id" in fields and "payment_format" in fields:
            self._process_data(fields)
        elif isinstance(fields, list) and len(fields) == 1:
            logging.info(f"EOF detectado en la cola de datos. Retransmitiendo a todas las instancias para {fields[0][:8]}")
            self.control_exchange_publisher.send(message)
        
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]

        self.input_queue.ch.connection.add_callback_threadsafe(
            lambda: self._process_eof(client_id)
        )
        ack()

    def handle_sigterm(self, signum, frame):
        logging.info("SIGTERM recibido")
        self.input_queue.stop_consuming()
        self.control_exchange_consumer.ch.connection.add_callback_threadsafe(
            self.control_exchange_consumer.stop_consuming
        )

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        
        # Hilo EOF
        self.thread_pcm = threading.Thread(
            target=self.control_exchange_consumer.start_consuming,
            args=(self.process_control_message,)
        )
        self.thread_pcm.start()

        self.input_queue.start_consuming(self.process_data_messsage)
        
        self.thread_pcm.join()

        self.input_queue.close()
        self.control_exchange_consumer.close()
        self.control_exchange_publisher.close()
        for exchange in self.data_output_exchanges:
            exchange.close()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    counter = PaymentMethodCounter()
    counter.start()
    return 0

if __name__ == "__main__":
    main()