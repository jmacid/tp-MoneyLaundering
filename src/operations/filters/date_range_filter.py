from datetime import datetime
import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy

class DateRangeFilter(OperationStrategy):

    def __init__(self, start_date: datetime | None = None, end_date: datetime | None = None):
        self.start_date = start_date or datetime.fromisoformat(os.getenv("START_DATE"))
        self.end_date = end_date or datetime.fromisoformat(os.getenv("END_DATE"))

        if start_date is None or end_date is None:
            raise ValueError("Missing START_DATE or END_DATE")


    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        if self.start_date <= transaction["timestamp"] and transaction["timestamp"] <= self.end_date:
            return transaction
