import json
import os
import signal
import logging
from dispatchers.exchange_dispatcher import ExchangeDispatcher
from dispatchers.projection_dispatcher import ProjectionDispatcher
from dispatchers.queue_dispatcher import QueueDispatcher
from operations.core.operation_factory import OperationFactory
from domain.message_type import MessageType
from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

logging.basicConfig(level=logging.INFO)

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
    "bank_resolver"
]

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
    elif operation_type not in ALLOWED_OPERATIONS:
        raise ValueError(f"Unsupported operation type: {operation_type}")
    elif operation_type == "projection_dispatcher":
        return ProjectionDispatcher()
    return OperationFactory.create(operation_type)

def initialize_dispatcher():
    middleware_type = os.getenv("OUTPUT_MIDDLEWARE_TYPE", "queue")
    if middleware_type == "queue":
        return QueueDispatcher()
    if middleware_type == "exchange":
        return ExchangeDispatcher()
    raise ValueError(f"Unsupported OUTPUT_MIDDLEWARE_TYPE: {middleware_type}")

def initialize_input_middleware():
    queue_name = os.getenv("INPUT_QUEUE")
    if not queue_name:
        raise ValueError("Missing INPUT_QUEUE environment variable")
    return MessageMiddlewareQueueRabbitMQ(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        queue_name=queue_name,
    )

def initialize_eof_middleware():
    eof_handler_queue = os.getenv("EOF_HANDLER_QUEUE")
    if not eof_handler_queue:
        raise ValueError("Missing EOF_HANDLER_QUEUE environment variable")
    return MessageMiddlewareQueueRabbitMQ(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        queue_name=eof_handler_queue,
    )

def handle_eof(message: dict, operation, eof_middleware) -> None:
    client_id = message.get("client_id")
    query_id = message.get("query_id")
    node_name = os.getenv("OPERATION_TYPE")

    # Si la operacion tiene estado, hacer flush pasando el client_id
    if hasattr(operation, "flush"):
        logging.info(f"Flushing operation for client {client_id} query {query_id}")
        operation.flush(client_id)

    # Responder Ready al EOF Handler
    ready_message = json.dumps({
        "type": "ready",
        "client_id": client_id,
        "query_id": query_id,
        "node_name": node_name,
    })
    eof_middleware.send(ready_message)
    logging.info(f"Sent Ready for client {client_id} query {query_id}")

def main():
    operation = build_operation()
    dispatcher = (
        None
        if isinstance(operation, ProjectionDispatcher)
        else initialize_dispatcher()
    )
    input_middleware = initialize_input_middleware()
    eof_middleware = initialize_eof_middleware()

    logging.info(f"Initialized successfully operation: {os.getenv('OPERATION_TYPE')}")

    while running:
        message = input_middleware.consume()
        if message is None:
            continue

        msg_type = message.get("type")

        if msg_type == MessageType.TRANSACTION:
            result = operation.process(message)
            if dispatcher is not None and result is not None:
                dispatcher.process([result])

        elif msg_type == MessageType.EOF:
            handle_eof(message, operation, eof_middleware)

        else:
            logging.warning(f"Unknown message type: {msg_type}")

    logging.info("Worker shutting down cleanly")

if __name__ == "__main__":
    main()