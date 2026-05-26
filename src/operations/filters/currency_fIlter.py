import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator

class CurrencyFilter(OperationStrategy):

    def __init__(self, currency: str | None = None):
        self.currency = currency or os.getenv("CURRENCY")

        if not self.currency:
            raise ValueError("Currency not provided and CURRENCY env var is not defined")
        
        self.required_fields = ["payment_currency", "receiving_currency"]

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:

        TransactionValidator.validate_required_fields(transaction, self.required_fields)

        if transaction["payment_currency"] == self.currency or transaction["receiving_currency"] == self.currency:
            return transaction
