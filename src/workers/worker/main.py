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


    
    if os.getenv("OPERATION_TYPE") != "projection_dispatcher":

        def handle_message(transaction):

            logging.info(
                "Processing transaction: %s",
                transaction,
            )

            result = operation.process(transaction)
            logging.info(f"Processed transaction: {result}")

            if result is None:
                return
            
            dispatcher.process([result])
        
        consumer.start(handle_message)

    else:
        transaction = {
            "timestamp": "2026-05-24T15:30:00",

            "from_bank": "GALICIA",
            "from_account": "AR123456789",

            "to_bank": "SANTANDER",
            "to_account": "AR987654321",

            "amount_received": "1250.75",
            "receiving_currency": "USD",

            "amount_paid": "1500000.00",
            "payment_currency": "ARS",

            "payment_format": "Wire",

            "is_laundering": False,

            "normalized_amount_paid": "1250.75",
            "normalized_amount_received": "1250.75",
            "normalized_currency": "USD",
        }

        result = operation.process(transaction)

        if dispatcher is not None and result is not None:
            dispatcher.process([result])    

if __name__ == "__main__":
    main()