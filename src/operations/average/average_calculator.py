from collections import defaultdict
from decimal import Decimal
import logging
import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator

class AverageCalculator(OperationStrategy):

    def __init__(self, group_by: str | None = None):
        group_by_raw = os.getenv("AVERAGE_GROUP_BY")

        if group_by is None and not group_by_raw:
            raise ValueError("Missing environment variable: AVERAGE_GROUP_BY")

        self.avg_group = (group_by or group_by_raw)
        self.required_fields = {self.avg_group, "amount_paid", "client_id"}

        self.stats: dict[str, dict[str, dict[str, Decimal | int]]] = defaultdict(
            lambda: defaultdict(lambda: {"count": 0, "sum": Decimal("0")})
        )

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        TransactionValidator.validate_required_fields(transaction, self.required_fields)

        client_id = transaction["client_id"]
        grouped_by = transaction[self.avg_group]

        self.stats[client_id][grouped_by]["count"] += 1
        self.stats[client_id][grouped_by]["sum"] += Decimal(str(transaction["amount_paid"]))

        return None