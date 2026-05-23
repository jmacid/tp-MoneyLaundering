from datetime import datetime
from decimal import Decimal

from domain.transaction import Transaction
from operations.core.operation_factory import OperationFactory


def main():
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

    operations_config = [
        {
            "type": "currency",
            "params": {
                "currency": "USD",
            },
        },
        {
            "type": "amount",
            "params": {
                "minimum_amount": Decimal("1000"),
            },
        },
        {
            "type": "date_range",
            "params": {
                "start_date": datetime(2022, 9, 1),
                "end_date": datetime(2022, 9, 5),
            },
        },
        {
            "type": "payment_method",
            "params": {
                "payment_method": "ACH",
            },
        },
    ]

    print("Transaction:")
    print(transaction)

    print("\nFactory dynamic operations result:")

    for operation_config in operations_config:
        operation = OperationFactory.create(
            operation_config["type"],
            **operation_config["params"],
        )

        result = operation.process(transaction)

        print(f"{operation_config['type']}: {result}")


if __name__ == "__main__":
    main()