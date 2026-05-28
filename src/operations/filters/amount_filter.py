from decimal import Decimal
import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator

class AmountFilter(OperationStrategy):
    def __init__(self):
        amount_raw = os.getenv("AMOUNT")
        if not amount_raw:
            raise ValueError("Missing environment variable: AMOUNT")
        self.amount = Decimal(amount_raw)
        self.mode = os.getenv("FILTER_MODE", "lt")  # lt = menor a, gte = mayor o igual
        self.required_fields = ["amount_paid"]

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        TransactionValidator.validate_required_fields(transaction, self.required_fields)
        value = Decimal(transaction["amount_paid"])
        if self.mode == "lt" and value < self.amount:
            return transaction
        if self.mode == "gte" and value >= self.amount:
            return transaction
        return None