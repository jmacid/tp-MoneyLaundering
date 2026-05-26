import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy

class CurrencyFilter(OperationStrategy):

    def __init__(self, currency: str | None = None):
        self.currency = currency or os.getenv("CURRENCY")

        if not self.currency:
            raise ValueError(
                "Currency not provided and CURRENCY env var is not defined"
            )

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        if transaction["payment_currency"] == self.currency:
            return transaction
