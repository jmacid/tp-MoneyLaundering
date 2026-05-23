from datetime import datetime
from operations.core.operation_strategy import OperationStrategy
from domain.transaction import Transaction

class DateRangeFilter(OperationStrategy):

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date = end_date

    def process(self, transaction: Transaction) -> bool:
        return self.start_date <= transaction.timestamp <= self.end_date