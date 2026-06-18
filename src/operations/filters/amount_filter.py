from decimal import Decimal
import os
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator
from common.message_protocol.transaction_batch import TransactionBatch
import logging

class AmountFilter(OperationStrategy):

    def __init__(self, minimum_amount: Decimal | None = None):

        minimum_amount_raw = os.getenv("MINIMUM_AMOUNT")
        self.amount_field = os.getenv("AMOUNT_FIELD", "amount_paid")

        if minimum_amount is None and not minimum_amount_raw:
            raise ValueError("Missing environment variable: MINIMUM_AMOUNT")

        self.minimum_amount = minimum_amount or Decimal(minimum_amount_raw)
        logging.info(f"minimum_amount configurado: {self.minimum_amount}")
        self.required_fields = [self.amount_field]

    def process(self, batch: TransactionBatch) -> TransactionBatch | None:
        filtered_lines = []

        for transaction in batch.lines:
            TransactionValidator.validate_required_fields(transaction, self.required_fields)

            try:
                transaction_amount = Decimal(str(transaction[self.amount_field]))
                
                if transaction_amount < self.minimum_amount:
                    filtered_lines.append(transaction)
                    
            except Exception as e:
                logging.error(f"Error parsing amount in field {self.amount_field}: {e}")
                continue

        if not filtered_lines and not batch.is_last:
            return None

        return TransactionBatch(
            sequence_number=batch.sequence_number,
            lines=filtered_lines,
            is_last=batch.is_last,
            client_id=batch.client_id
        )