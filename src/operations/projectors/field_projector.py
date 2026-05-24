from typing import Any
from domain.transaction_fields import TRANSACTION_FIELDS
from operations.core.operation_strategy import OperationStrategy

class FieldProjector(OperationStrategy):

    def __init__(self, fields: list[str]):
        invalid_fields = set(fields) - TRANSACTION_FIELDS

        if invalid_fields:
            raise ValueError(f"Invalid projection fields: {invalid_fields}")

        self.fields = fields

    def process(self, transaction) -> dict[str, Any] | None:
        return {
            field: getattr(transaction, field)
            for field in self.fields
        }