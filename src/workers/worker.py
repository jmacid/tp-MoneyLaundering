import os
from decimal import Decimal
from datetime import datetime
from domain.transaction import Transaction
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


def main():
    operation = build_operation()

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