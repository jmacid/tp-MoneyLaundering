import json
import logging
import os
import signal
import yaml
from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

logging.basicConfig(level=logging.INFO)

class EOFHandler:
    def __init__(self, config_path: str):
        self.running = True
        self.pipeline = self._load_config(config_path)
        
        # client_id -> query_id -> nodo actual en la pipeline
        self.client_state: dict[str, dict[str, int]] = {}
        
        # client_id -> query_id -> nodo -> cantidad de Readys recibidos
        self.ready_counts: dict[str, dict[str, dict[str, int]]] = {}

        self.input_middleware = MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=os.getenv("EOF_HANDLER_QUEUE", "eof_handler"),
        )

        self.gateway_middleware = MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=os.getenv("GATEWAY_QUEUE", "gateway"),
        )

        # Una middleware por cada cola de EOF y Ready de cada nodo
        self.eof_middlewares: dict[str, MessageMiddlewareQueueRabbitMQ] = {}
        self.ready_middlewares: dict[str, MessageMiddlewareQueueRabbitMQ] = {}
        self._init_middlewares()

    def _load_config(self, config_path: str) -> dict:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def _init_middlewares(self) -> None:
        for query_id, query_config in self.pipeline["queries"].items():
            for node in query_config["pipeline"]:
                eof_queue = node["eof_queue"]
                ready_queue = node["ready_queue"]

                if eof_queue not in self.eof_middlewares:
                    self.eof_middlewares[eof_queue] = MessageMiddlewareQueueRabbitMQ(
                        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                        queue_name=eof_queue,
                    )
                if ready_queue not in self.ready_middlewares:
                    self.ready_middlewares[ready_queue] = MessageMiddlewareQueueRabbitMQ(
                        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                        queue_name=ready_queue,
                    )

    def _send_eof_to_node(self, node: dict, client_id: str, query_id: str) -> None:
        message = json.dumps({
            "type": "eof",
            "client_id": client_id,
            "query_id": query_id,
        })
        instances = node["instances"]
        for _ in range(instances):
            self.eof_middlewares[node["eof_queue"]].send(message)
        logging.info(f"Sent EOF to {node['name']} ({instances} instances) for client {client_id} query {query_id}")

    def _handle_eof(self, message: dict) -> None:
        client_id = message.get("client_id")
        query_id = message.get("query_id")

        # Inicializar estado del cliente si es nuevo
        if client_id not in self.client_state:
            self.client_state[client_id] = {}
            self.ready_counts[client_id] = {}

        if query_id not in self.client_state[client_id]:
            self.client_state[client_id][query_id] = 0
            self.ready_counts[client_id][query_id] = {}

        # Enviar EOF al primer nodo de la pipeline
        pipeline = self.pipeline["queries"][query_id]["pipeline"]
        first_node = pipeline[0]
        self._send_eof_to_node(first_node, client_id, query_id)

    def _handle_ready(self, message: dict) -> None:
        client_id = message.get("client_id")
        query_id = message.get("query_id")
        node_name = message.get("node_name")

        pipeline = self.pipeline["queries"][query_id]["pipeline"]
        current_index = self.client_state[client_id][query_id]
        current_node = pipeline[current_index]

        # Contar Readys recibidos para este nodo
        if node_name not in self.ready_counts[client_id][query_id]:
            self.ready_counts[client_id][query_id][node_name] = 0
        self.ready_counts[client_id][query_id][node_name] += 1

        expected_readys = current_node["instances"]
        received_readys = self.ready_counts[client_id][query_id][node_name]

        logging.info(f"Ready {received_readys}/{expected_readys} from {node_name} for client {client_id} query {query_id}")

        if received_readys < expected_readys:
            return

        # Todos los Readys recibidos, avanzar al siguiente nodo
        next_index = current_index + 1
        self.client_state[client_id][query_id] = next_index

        if next_index >= len(pipeline):
            # Pipeline completa para esta query
            logging.info(f"Pipeline complete for client {client_id} query {query_id}")
            self._check_all_queries_done(client_id)
            return

        next_node = pipeline[next_index]
        self._send_eof_to_node(next_node, client_id, query_id)

    def _check_all_queries_done(self, client_id: str) -> None:
        total_queries = len(self.pipeline["queries"])
        completed = sum(
            1 for query_id in self.client_state[client_id]
            if self.client_state[client_id][query_id] >= len(self.pipeline["queries"][query_id]["pipeline"])
        )
        if completed == total_queries:
            logging.info(f"All queries complete for client {client_id}, notifying gateway")
            self.gateway_middleware.send(json.dumps({
                "type": "eof",
                "client_id": client_id,
            }))
            # Limpiar estado del cliente
            del self.client_state[client_id]
            del self.ready_counts[client_id]

    def handle(self, message: dict) -> None:
        msg_type = message.get("type")
        if msg_type == "eof":
            self._handle_eof(message)
        elif msg_type == "ready":
            self._handle_ready(message)
        else:
            logging.warning(f"Unknown message type: {msg_type}")

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        logging.info("EOF Handler started")

        while self.running:
            message = self.input_middleware.consume()
            if message is None:
                continue
            self.handle(message)

        logging.info("EOF Handler shutting down cleanly")

    def _handle_sigterm(self, sig, frame) -> None:
        logging.info("Received SIGTERM")
        self.running = False


if __name__ == "__main__":
    config_path = os.getenv("PIPELINE_CONFIG", "eof_handler/pipeline_config.yaml")
    handler = EOFHandler(config_path)
    handler.run()