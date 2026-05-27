from datetime import datetime
import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator
import logging

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
        logging.info(f"Start date: {self.start_date}")
        logging.info(f"End date: {self.end_date}")
        self.required_fields = ["timestamp"]

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:

        TransactionValidator.validate_required_fields(transaction, self.required_fields)
        
        try:
            timestamp = datetime.strptime(transaction["timestamp"], "%Y/%m/%d %H:%M")
            pass
        except Exception as e:
            logging.error(f"Timestamp: {timestamp} could not be formated")
            return None


        if self.start_date <= timestamp <= self.end_date:
            return transaction

        return None