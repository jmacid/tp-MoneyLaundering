from typing import Any
from operations.core.operation_strategy import OperationStrategy


class PaymentMethodCounter(OperationStrategy):

    REQUIRED_FIELDS = {
        "client_id",
        "payment_format",
    }

    def __init__(self):
        self.count = {}

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        self._validate_transaction(transaction)

        client_id = transaction["client_id"]
        payment_method = transaction["payment_format"]

        if client_id not in self.count:
            self.count[client_id] = {}

        if payment_method not in self.count[client_id]:
            self.count[client_id][payment_method] = 0

        self.count[client_id][payment_method] += 1

    def _validate_transaction(self, transaction: dict) -> None:
        missing_fields = self.REQUIRED_FIELDS - transaction.keys()

        if missing_fields:
            raise ValueError(
                f"Missing required fields: {missing_fields}"
            )

    def get_count(self):
        return self.count
        
