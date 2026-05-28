# workers/worker/main.py
import json
import logging
import os
import signal
from typing import Any

from workers.consumers.queue_consumer import QueueConsumer
from workers.dispatchers.queue_dispatcher import QueueDispatcher
from workers.dispatchers.projection_dispatcher import ProjectionDispatcher
from workers.dispatchers.bank_dispatcher import BankDispatcher
from operations.core.operation_factory import OperationFactory
from domain.message_type import MessageType
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

ALLOWED_OPERATIONS = [
    "currency_filter",
    "amount_filter",
    "date_range_filter",
    "payment_method_filter",
    "payment_method_counter",
    "currency_normalizer",
    "projection_dispatcher",
    "bank_dispatcher",
    "local_bank_max_aggregator",
    "bank_resolver",
]

SELF_DISPATCHING_OPERATIONS = {"projection_dispatcher", "bank_dispatcher", "payment_method_counter"}

running = True

def handle_sigterm(sig, frame):
    global running
    logging.info("Received SIGTERM, shutting down...")
    running = False

signal.signal(signal.SIGTERM, handle_sigterm)


def build_operation():
    operation_type = os.getenv("OPERATION_TYPE")
    if operation_type is None:
        raise ValueError("Missing environment variable: OPERATION_TYPE")
    if operation_type not in ALLOWED_OPERATIONS:
        raise ValueError(f"Unsupported operation type: {operation_type}")
    if operation_type == "projection_dispatcher":
        return ProjectionDispatcher()
    if operation_type == "bank_dispatcher":
        return BankDispatcher()
    return OperationFactory.create(operation_type)


def initialize_dispatcher():
    operation_type = os.getenv("OPERATION_TYPE")
    if operation_type in SELF_DISPATCHING_OPERATIONS:
        return None
    return QueueDispatcher()


def initialize_eof_middleware():
    eof_queue = os.getenv("EOF_HANDLER_QUEUE")
    if not eof_queue:
        raise ValueError("Missing EOF_HANDLER_QUEUE")
    return MessageMiddlewareQueueRabbitMQ(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        queue_name=eof_queue,
    )


def send_ready(eof_middleware, client_id: str, query_id: str) -> None:
    message = json.dumps({
        "type": "ready",
        "client_id": client_id,
        "query_id": query_id,
        "node_name": os.getenv("NODE_NAME"),
    })
    eof_middleware.send(message)
    logging.info(f"Sent READY for client {client_id} query {query_id}")


def main():
    operation = build_operation()
    dispatcher = initialize_dispatcher()
    consumer = QueueConsumer()
    eof_middleware = initialize_eof_middleware()

    operation_type = os.getenv("OPERATION_TYPE")
    logging.info(f"Initialized successfully operation: {operation_type}")

    def handle_message(message: dict[str, Any]) -> None:
        msg_type = message.get("type")

        if msg_type == MessageType.EOF:
            client_id = message.get("client_id")
            query_id = message.get("query_id")
            if hasattr(operation, "flush"):
                operation.flush(client_id)  # flush solo recibe client_id
            send_ready(eof_middleware, client_id, query_id)
            return

        if operation_type in SELF_DISPATCHING_OPERATIONS:
            operation.process(message)
            return

        result = operation.process(message)
        if result is None:
            return
        dispatcher.process([result])

    consumer.start(handle_message)


if __name__ == "__main__":
    main()