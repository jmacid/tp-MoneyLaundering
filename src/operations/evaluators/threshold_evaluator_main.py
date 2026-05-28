import os
import logging
import threading
import signal
from common import middleware, message_protocol

MOM_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
TX_QUEUE = os.getenv("TX_QUEUE", "threshold_evaluation_3")
AVG_QUEUE = os.getenv("AVG_QUEUE", "calculated_averages_3")
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE", "threshold_transactions_3")

class ThresholdEvaluator:
    def __init__(self):
        self.tx_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, TX_QUEUE)
        self.avg_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, AVG_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

        self.transactions_by_client = {}
        self.averages_by_client = {}
        self.tx_eof_received = set()
        self.lock = threading.Lock()

    def _evaluate(self, client_id):
        if client_id in self.tx_eof_received and client_id in self.averages_by_client:
            logging.info(f"Evaluando transacciones para cliente {client_id[:8]}")
            averages = self.averages_by_client[client_id]
            transactions = self.transactions_by_client.get(client_id, [])

            for tx in transactions:
                fmt = tx.get("payment_format")
                if not fmt or fmt not in averages:
                    continue
                
                avg_data = averages[fmt]
                avg_value = avg_data["sum"] / avg_data["count"] if avg_data["count"] > 0 else 0
                
                amount = float(tx.get("amount_paid", 0))

                if amount < (avg_value / 100):
                    self.output_queue.send(message_protocol.internal.serialize(tx))

            logging.info(f"Evaluación terminada para {client_id[:8]}. Enviando EOF a Gateway.")
            self.output_queue.send(message_protocol.internal.serialize([client_id]))

            del self.transactions_by_client[client_id]
            del self.averages_by_client[client_id]
            self.tx_eof_received.remove(client_id)

    def process_tx_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if isinstance(fields, dict) and "client_id" in fields:
                client_id = fields["client_id"]
                with self.lock:
                    if client_id not in self.transactions_by_client:
                        self.transactions_by_client[client_id] = []
                    self.transactions_by_client[client_id].append(fields)
                    logging.info(f"Guardando TX en RAM. Cliente: {client_id[:8]} | Esperando promedio...")
            elif isinstance(fields, list) and len(fields) == 1:
                client_id = fields[0]
                with self.lock:
                    self.tx_eof_received.add(client_id)
                    self._evaluate(client_id)
            ack()
        except Exception as e:
            logging.error(f"Error process_tx: {e}")
            nack()

    def process_avg_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            logging.info(f"fields: {fields}")
            if isinstance(fields, dict) and "client_id" in fields and "counts" in fields:
                client_id = fields["client_id"]
                with self.lock:
                    self.averages_by_client[client_id] = fields["counts"]
                    logging.info(f"¡Promedio recibido del Joiner para {client_id[:8]}! Listo para cruzar datos.")
                    self._evaluate(client_id)
            ack()
        except Exception as e:
            logging.error(f"Error process_avg: {e}")
            nack()

    def handle_sigterm(self, signum, frame):
        self.tx_queue.stop_consuming()
        self.avg_queue.ch.connection.add_callback_threadsafe(self.avg_queue.stop_consuming)

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        
        self.thread_avg = threading.Thread(
            target=self.avg_queue.start_consuming,
            args=(self.process_avg_message,)
        )
        self.thread_avg.start()

        self.tx_queue.start_consuming(self.process_tx_message)

        self.thread_avg.join()
        self.tx_queue.close()
        self.avg_queue.close()
        self.output_queue.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ThresholdEvaluator().start()