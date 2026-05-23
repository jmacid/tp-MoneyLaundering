import os
from decimal import Decimal
from datetime import datetime
from domain.transaction import Transaction
from middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from operations.core.operation_factory import OperationFactory

def build_operation():
    operation_type = os.getenv("OPERATION_TYPE")

    if operation_type is None:
        raise ValueError("Missing environment variable: OPERATION_TYPE")

    params = {}

    if operation_type == "currency":
        params["currency"] = os.getenv("CURRENCY")

    elif operation_type == "amount":
        amount = os.getenv("MINIMUM_AMOUNT")
        if amount is None:
            raise ValueError("Missing environment variable: MINIMUM_AMOUNT")

        params["minimum_amount"] = Decimal(amount)

    elif operation_type == "date_range":
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")

        if start_date is None or end_date is None:
            raise ValueError("Missing START_DATE or END_DATE")

        params["start_date"] = datetime.fromisoformat(start_date)
        params["end_date"] = datetime.fromisoformat(end_date)

    elif operation_type == "destination":
        params["destination_bank"] = os.getenv("DESTINATION_BANK")

    elif operation_type == "payment_method":
        params["payment_method"] = os.getenv("PAYMENT_METHOD")

    else:
        raise ValueError(f"Unsupported operation type: {operation_type}")

    return OperationFactory.create(operation_type, **params)

def initialize_middleware(direction: str):
    prefix = direction.upper()

    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    middleware_type = os.getenv(f"{prefix}_MIDDLEWARE_TYPE", "queue")

    if middleware_type == "queue":
        queue_name = os.getenv(f"{prefix}_QUEUE")

        if queue_name is None:
            raise ValueError(f"Missing environment variable: {prefix}_QUEUE")

        return MessageMiddlewareQueueRabbitMQ(
            host=rabbitmq_host,
            queue_name=queue_name,
        )

    if middleware_type == "exchange":
        exchange_name = os.getenv(f"{prefix}_EXCHANGE")
        routing_keys_raw = os.getenv(f"{prefix}_ROUTING_KEYS", "")

        if exchange_name is None:
            raise ValueError(f"Missing environment variable: {prefix}_EXCHANGE")

        routing_keys = [
            key.strip()
            for key in routing_keys_raw.split(",")
            if key.strip()
        ]

        return MessageMiddlewareExchangeRabbitMQ(
            host=rabbitmq_host,
            exchange_name=exchange_name,
            routing_keys=routing_keys,
        )

    raise ValueError(
        f"Unsupported {prefix}_MIDDLEWARE_TYPE: {middleware_type}"
    )

def main():
    operation = build_operation()
    
    initialize_middleware("input")
    initialize_middleware("output")

    transaction = Transaction(
        timestamp=datetime(2022, 9, 1, 10, 30),
        from_bank="Bank A",
        from_account="ACC-001",
        to_bank="Bank B",
        to_account="ACC-999",
        amount_received=Decimal("1000"),
        receiving_currency="EUR",
        amount_paid=Decimal("1080"),
        payment_currency="USD",
        payment_format="ACH",
        is_laundering=False,
    )

    result = operation.process(transaction)

    print("Worker operation:")
    print(operation.__class__.__name__)

    print("\nResult:")
    print(result)


if __name__ == "__main__":
    main()