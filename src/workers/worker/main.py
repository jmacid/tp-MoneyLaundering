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

EOF_CONTROL_QUEUES = [q.strip() for q in os.getenv("EOF_CONTROL_QUEUE", "eof_control_queue").split(",") if q.strip()]
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

    control_queues = [
        middleware.MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, q)
        for q in EOF_CONTROL_QUEUES
    ]

    node_name = os.getenv("NODE_NAME", os.getenv("OPERATION_TYPE"))

    logging.info(f"Initialized successfully operation: {os.getenv("OPERATION_TYPE")}")

    def handle_message(message):
        # EOF signal: ['client-id-string']
        if isinstance(message, list) and len(message) == 1 and isinstance(message[0], str):
            client_id = message[0]
            if isinstance(operation, BankDispatcher):
                operation.process(message)
            control_msg = json.dumps({
                "client_id": client_id,
                "node": node_name,
                "processed": 0,
                "emitted": 0
            })
            for control_queue in control_queues:
                control_queue.send(control_msg.encode('utf-8'))
            return

        items = message if isinstance(message, list) and message and isinstance(message[0], dict) else [message]
        client_id = items[0].get("client_id") if items else None

        if isinstance(operation, (ProjectionDispatcher, BankDispatcher)):
            operation.process_batch(items)
            total_processed = len(items)
            total_emitted = len(items) if isinstance(operation, ProjectionDispatcher) else 0
        else:
            batch_results = []
            for transaction in items:
                result = operation.process(transaction)
                if result is not None:
                    logging.info(f"Processed transaction result: {result}")
                    batch_results.append(result)

            total_processed = len(items)
            total_emitted = len(batch_results)

            if batch_results and dispatcher is not None:
                dispatcher.dispatch_batch(batch_results)

        if client_id is not None:
            control_msg = json.dumps({
                "client_id": client_id,
                "node": node_name,
                "processed": total_processed,
                "emitted": total_emitted
            })
            for control_queue in control_queues:
                control_queue.send(control_msg.encode('utf-8'))

    consumer.start(handle_message)

if __name__ == "__main__":
    main()