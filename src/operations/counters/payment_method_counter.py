import logging
from typing import Any
from operations.core.operation_strategy import OperationStrategy


class PaymentMethodCounter(OperationStrategy):

    REQUIRED_FIELDS = {
        #"client_id", only for demo
        "payment_format",
    }

    def __init__(self):
        self.count = {}

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        self._validate_transaction(transaction)

        client_id = "Hardcoded for demo" # transaction["client_id"] for DEMO
        payment_method = transaction["payment_format"]

        logging.info(f"Processing Client={client_id} Payment={payment_method}")

        if client_id not in self.count:
            self.count[client_id] = {}

        if payment_method not in self.count[client_id]:
            self.count[client_id][payment_method] = 0

        self.count[client_id][payment_method] += 1

        logging.info(f"Client={client_id} Payment={payment_method} Count {self.count[client_id][payment_method]}")

    def _validate_transaction(self, transaction: dict) -> None:
        missing_fields = self.REQUIRED_FIELDS - transaction.keys()

        if missing_fields:
            raise ValueError(
                f"Missing required fields: {missing_fields}"
            )

    def get_count(self):
        return self.count
        
