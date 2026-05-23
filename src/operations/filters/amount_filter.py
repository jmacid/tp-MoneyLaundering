from decimal import Decimal
from operations.core.operation_strategy import OperationStrategy
from domain.transaction import Transaction

class AmountFilter(OperationStrategy):

    def __init__(self, minimum_amount: Decimal):
        self.minimum_amount = minimum_amount

    def process(self, transaction: Transaction) -> bool:
        return transaction.amount_paid >= self.minimum_amount
