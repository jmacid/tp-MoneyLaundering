from operations.core.operation_strategy import OperationStrategy
from domain.transaction import Transaction

class CurrencyFilter(OperationStrategy):

    def __init__(self, currency: str):
        self.currency = currency

    def process(self, transaction: Transaction) -> bool:
        return (
            transaction.payment_currency == self.currency
            or transaction.receiving_currency == self.currency
        )
