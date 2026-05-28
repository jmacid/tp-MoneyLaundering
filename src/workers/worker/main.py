import os
import logging
from workers.consumers.exchange_consumer import ExchangeConsumer
from workers.consumers.queue_consumer import QueueConsumer
from workers.dispatchers.exchange_dispatcher import ExchangeDispatcher
from workers.dispatchers.projection_dispatcher import ProjectionDispatcher
from workers.dispatchers.queue_dispatcher import QueueDispatcher
from workers.dispatchers.sharding_dispatcher import ShardingDispatcher
from operations.core.operation_factory import OperationFactory
from workers.dispatchers.broadcast_dispatcher import BroadcastDispatcher
from workers.dispatchers.bank_dispatcher import BankDispatcher
import json
from common import middleware

ALLOWED_OPERATIONS = ["currency_filter","amount_filter","date_range_filter","payment_method_filter",
                      "payment_method_counter","currency_normalizer", "projection_dispatcher","bank_dispatcher",
                       "destination_filter", "scatter_gather_detector"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

EOF_CONTROL_QUEUE =os.getenv("EOF_CONTROL_QUEUE", "eof_control_queue")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

def build_operation():
    operation_type = os.getenv("OPERATION_TYPE")

    if operation_type is None:
        raise ValueError("Missing environment variable: OPERATION_TYPE")
    elif operation_type not in ALLOWED_OPERATIONS:
        raise ValueError(f"Unsupported operation type: {operation_type}")
    elif operation_type == "projection_dispatcher":
        return ProjectionDispatcher()
    elif operation_type == "bank_dispatcher":
        return BankDispatcher()

    return OperationFactory.create(operation_type)

def initialize_dispatcher():
    middleware_type = os.getenv("OUTPUT_MIDDLEWARE_TYPE", "queue")

    if middleware_type == "none":
        return None

    if middleware_type == "queue":
        return QueueDispatcher()

    if middleware_type == "exchange":
        return ExchangeDispatcher()

    if middleware_type == "sharding_exchange":
        return ShardingDispatcher()

    if middleware_type == "broadcast":
        return BroadcastDispatcher()

    raise ValueError(
        f"Unsupported OUTPUT_MIDDLEWARE_TYPE: {middleware_type}"
    )

def initialize_consumer():
    middleware_type = os.getenv("INPUT_MIDDLEWARE_TYPE", "queue")

    if middleware_type == "queue":
        return QueueConsumer()

    if middleware_type == "exchange":
        return ExchangeConsumer()

    raise ValueError(
        f"Unsupported OUTPUT_MIDDLEWARE_TYPE: {middleware_type}"
    )

def operation_handles_dispatch(operation) -> bool:
    return isinstance(operation, ProjectionDispatcher)

def main():

    operation = build_operation()
    dispatcher = initialize_dispatcher()
    consumer = initialize_consumer()

    control_queue = middleware.MessageMiddlewareQueueRabbitMQ(
        RABBITMQ_HOST,
        EOF_CONTROL_QUEUE
    )

    node_name = os.getenv("NODE_NAME", os.getenv("OPERATION_TYPE"))

    logging.info(f"Initialized successfully operation: {os.getenv("OPERATION_TYPE")}")

    def handle_message(transaction):
        # logging.info("Processing transaction: %s", transaction)

        result = operation.process(transaction)
        if result is not None:
            logging.info(f"Processed transaction result: {result}")

        if result is not None and dispatcher is not None:
            dispatcher.process([result])
        if isinstance(operation, ProjectionDispatcher):
            emitted_count = 1
        else:
            emitted_count = 1 if result is not None else 0

        control_msg = json.dumps({
            "client_id": transaction["client_id"],
            "node": node_name,
            "processed": 1,
            "emitted": emitted_count
        })
        control_queue.send(control_msg.encode('utf-8'))

    consumer.start(handle_message)

if __name__ == "__main__":
    main()