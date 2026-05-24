import os
from typing import Any

from operations.core.operation_strategy import OperationStrategy
from domain.transaction import Transaction

class PaymentMethodFilter(OperationStrategy):

    def __init__(self, payment_method: str | None = None):
        self.payment_method = payment_method or os.getenv("PAYMENT_METHOD")

        if self.payment_method is None:
            raise ValueError("Missing PAYMENT_METHOD")

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        if transaction["payment_format"] == self.payment_method:
            return transaction
