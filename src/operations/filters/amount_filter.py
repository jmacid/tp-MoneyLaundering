from decimal import Decimal
import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator
import logging

class AmountFilter(OperationStrategy):

    def __init__(self, minimum_amount: Decimal | None = None):

        minimum_amount_raw = os.getenv("MINIMUM_AMOUNT")
        self.amount_field = os.getenv("AMOUNT_FIELD", "amount_paid")

        if minimum_amount is None and not minimum_amount_raw:
            raise ValueError("Missing environment variable: MINIMUM_AMOUNT")

        self.minimum_amount = (minimum_amount or Decimal(minimum_amount_raw))
        logging.info(f"minimum_amount: {minimum_amount}")
        self.required_fields = [self.amount_field]

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:

        TransactionValidator.validate_required_fields(transaction, self.required_fields)

        logging.info(f"transaction amount_paid: {transaction[self.amount_field]} - {self.minimum_amount}")
        if Decimal(transaction[self.amount_field]) < self.minimum_amount:
            return transaction
        return None