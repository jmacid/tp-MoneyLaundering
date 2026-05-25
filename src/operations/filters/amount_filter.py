from decimal import Decimal
import os
from typing import Any
import logging
from operations.core.operation_strategy import OperationStrategy

class AmountFilter(OperationStrategy):

    def __init__(self, minimum_amount: Decimal | None = None):
        self.minimum_amount = minimum_amount or Decimal(os.getenv("MINIMUM_AMOUNT"))

        if self.minimum_amount is None:
            raise ValueError("Missing environment variable: MINIMUM_AMOUNT")

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:

        logging.info(f"Comparing {transaction["amount_paid"]} with {self.minimum_amount}")
        if Decimal(transaction["amount_paid"]) >= self.minimum_amount:
            return transaction
