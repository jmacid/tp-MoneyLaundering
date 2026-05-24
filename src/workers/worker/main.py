import os
from dispatchers import ExchangeDispatcher, ProjectionDispatcher, QueueDispatcher
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
    
    #transaction = input_middleware.consume()
    result = None #operation.process(transaction)

    if dispatcher is not None and result is not None:
        dispatcher.process(result)


if __name__ == "__main__":
    main()