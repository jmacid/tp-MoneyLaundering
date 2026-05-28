import os
import logging
import signal
import threading
import json
from collections import defaultdict
from common import middleware, message_protocol

MOM_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
INPUT_QUEUE = os.getenv("INPUT_QUEUE", "bridge_account_shards_4")
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE", "scatter_gather_accounts_4")
EOF_CONTROL_QUEUE = os.getenv("EOF_CONTROL_QUEUE", "eof_control_queue_5")
NODE_NAME = os.getenv("OPERATION_TYPE", "scatter_gather_detector")

class ScatterGatherDetectorService:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        
        self.control_queue_consumer = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, EOF_CONTROL_QUEUE)
        self.control_queue_publisher = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, EOF_CONTROL_QUEUE)

        self.account_flow = defaultdict(lambda: defaultdict(lambda: {"incoming": set(), "outgoing": set()}))
        self.lock = threading.Lock()

    def _process_data(self, transaction):
        client_id = transaction["client_id"]
        from_account = transaction["from_account"]
        to_account = transaction["to_account"]

        with self.lock:
            self.account_flow[client_id][from_account]["outgoing"].add(to_account)
            self.account_flow[client_id][to_account]["incoming"].add(from_account)

    def _process_eof(self, client_id):
        logging.info(f"EOF recibido. Procesando grafo Scatter-Gather para cliente {client_id[:8]}")

        with self.lock:
            client_flow = self.account_flow.get(client_id, {})
            scatter_gather_paths = []

            for account, connections in client_flow.items():
                if len(connections["incoming"]) > 0 and len(connections["outgoing"]) > 0:
                    scatter_gather_paths.append({
                        "bridge_account": account,
                        "origins": list(connections["incoming"]),
                        "destinations": list(connections["outgoing"])
                    })

            if scatter_gather_paths:
                result = {
                    "client_id": client_id,
                    "scatter_gather_paths": scatter_gather_paths
                }
                self.output_queue.send(message_protocol.internal.serialize(result))

            if client_id in self.account_flow:
                del self.account_flow[client_id]

        eof_msg = json.dumps({"client_id": client_id, "node": NODE_NAME, "processed": 1, "emitted": 1})
        self.control_queue_publisher.send(eof_msg.encode('utf-8'))

    def process_data_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if isinstance(fields, dict) and "from_account" in fields and "to_account" in fields:
                self._process_data(fields)
            ack()
        except Exception as e:
            logging.error(f"Error procesando datos Scatter-Gather: {e}")
            nack()

    def process_control_message(self, message, ack, nack):
        try:
            msg = json.loads(message.decode('utf-8'))
            if isinstance(msg, dict) and msg.get("node") == "destination_filter": 
                client_id = msg.get("client_id")
                self.input_queue.ch.connection.add_callback_threadsafe(lambda: self._process_eof(client_id))
            ack()
        except Exception as e:
            nack()

    def handle_sigterm(self, signum, frame):
        self.input_queue.stop_consuming()
        self.control_queue_consumer.ch.connection.add_callback_threadsafe(self.control_queue_consumer.stop_consuming)

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        
        self.thread_pcm = threading.Thread(target=self.control_queue_consumer.start_consuming, args=(self.process_control_message,))
        self.thread_pcm.start()
        
        self.input_queue.start_consuming(self.process_data_message)
        
        self.thread_pcm.join()
        self.input_queue.close()
        self.output_queue.close()
        self.control_queue_consumer.close()
        self.control_queue_publisher.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ScatterGatherDetectorService().start()