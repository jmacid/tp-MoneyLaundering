import os
from typing import Any

from operations.core.operation_strategy import OperationStrategy
from domain.transaction import Transaction

class PaymentMethodFilter(OperationStrategy):

    def __init__(self, payment_methods: list[str] | None = None):

        payment_methods_raw = os.getenv(
            "PAYMENT_METHODS"
        )

        if payment_methods is None and not payment_methods_raw:
            raise ValueError(
                "Missing PAYMENT_METHODS"
            )

        self.payment_methods = (
            payment_methods
            or [
                py.strip()
                for py in payment_methods_raw.split(",")
                if py.strip()
            ]
        )

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        if (transaction["payment_format"] in self.payment_methods):
            return transaction
