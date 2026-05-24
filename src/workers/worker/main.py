import os
import logging
from dispatchers.exchange_dispatcher import ExchangeDispatcher
from dispatchers.projection_dispatcher import ProjectionDispatcher
from dispatchers.queue_dispatcher import QueueDispatcher
from operations.core.operation_factory import OperationFactory

ALLOWED_OPERATIONS = ["currency_filter","amount_filter","date_range_filter","payment_method_filter",
                      "payment_method_counter","currency_normalizer", "projection_dispatcher"]

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

def operation_handles_dispatch(operation) -> bool:
    return isinstance(operation, ProjectionDispatcher)

def main():

    operation = build_operation()
    dispatcher = initialize_dispatcher()

    dispatcher = (
        None
        if isinstance(operation, ProjectionDispatcher)
        else initialize_dispatcher()
    )

    logging.info(f"Initialized successfully operation: {os.getenv("OPERATION_TYPE")}")
    
    #transaction = input_middleware.consume()
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

        "payment_format": "TRANSFER",

        "is_laundering": False,

        "normalized_amount_paid": "1250.75",
        "normalized_amount_received": "1250.75",
        "normalized_currency": "USD",
    }

    logging.info(f"Received transaction: {transaction}")

    result = operation.process(transaction)

    logging.info(f"Processed transaction: {result}")

    if dispatcher is not None and result is not None:
        dispatcher.process([result])


if __name__ == "__main__":
    main()