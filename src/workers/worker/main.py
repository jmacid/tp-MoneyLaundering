import os
import logging
from workers.consumers.exchange_consumer import ExchangeConsumer
from workers.consumers.queue_consumer import QueueConsumer
from workers.dispatchers.exchange_dispatcher import ExchangeDispatcher
from workers.dispatchers.projection_dispatcher import ProjectionDispatcher
from workers.dispatchers.queue_dispatcher import QueueDispatcher
from operations.core.operation_factory import OperationFactory

ALLOWED_OPERATIONS = ["currency_filter","amount_filter","date_range_filter","payment_method_filter",
                      "payment_method_counter","currency_normalizer", "projection_dispatcher"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

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

    dispatcher = (
        None
        if isinstance(operation, ProjectionDispatcher)
        else initialize_dispatcher()
    )

    logging.info(f"Initialized successfully operation: {os.getenv("OPERATION_TYPE")}")

    def handle_message(transaction):

        logging.info(
            "Processing transaction: %s",
            transaction,
        )

        result = operation.process(transaction)
        logging.info(f"Processed transaction result: {result}")

        if result is None:
            return

        dispatcher.process([result])

    consumer.start(handle_message)

if __name__ == "__main__":
    main()