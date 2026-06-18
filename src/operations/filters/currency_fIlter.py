import logging
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

        logging.info(f"DEBUG FILTRO - Buscando: {self.currency} | Datos: {batch.lines[0] if batch.lines else 'Vacío'}")

        filtered = [
            t for t in batch.lines
            if t["payment_currency"] == self.currency or
            t["receiving_currency"] == self.currency
        ]
        if not filtered and not batch.is_last:
            return None
        
        return TransactionBatch(
            sequence_number=batch.sequence_number, 
            lines=filtered, 
            is_last=batch.is_last, 
            client_id=batch.client_id
        )