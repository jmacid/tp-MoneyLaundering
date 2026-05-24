from datetime import datetime
import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy

class DateRangeFilter(OperationStrategy):

    def __init__(self, start_date: datetime | None = None, end_date: datetime | None = None):
        
        start_date_raw = os.getenv("START_DATE")
        end_date_raw = os.getenv("END_DATE")

        if start_date is None and not start_date_raw:
            raise ValueError("Missing START_DATE")

        if end_date is None and not end_date_raw:
            raise ValueError("Missing END_DATE")

        self.start_date = start_date or datetime.fromisoformat(start_date_raw)
        self.end_date = end_date or datetime.fromisoformat(end_date_raw)

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        timestamp = datetime.fromisoformat(transaction["timestamp"])

        if self.start_date <= timestamp <= self.end_date:
            return transaction

        return None