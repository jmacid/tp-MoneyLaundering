import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator
from common.message_protocol.transaction_batch import TransactionBatch

class CurrencyFilter(OperationStrategy):

    def __init__(self, currency: str | None = None):
        self.currency = currency or os.getenv("CURRENCY")

        if not self.currency:
            raise ValueError("Currency not provided and CURRENCY env var is not defined")
        
        self.required_fields = ["payment_currency", "receiving_currency"]

    def process(self, batch: TransactionBatch) -> TransactionBatch | None:
        filtered = [
            t for t in batch.lines
            if t["payment_currency"] == self.currency or
            t["receiving_currency"] == self.currency
        ]
        if not filtered:
            return None 
        return TransactionBatch(batch.sequence_number, filtered, batch.is_last, batch.client_id)