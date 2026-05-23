from operations.core.operation_strategy import OperationStrategy
from domain.transaction import Transaction

class PaymentMethodFilter(OperationStrategy):

    def __init__(self, payment_method: str):
        self.payment_method = payment_method

    def process(self, transaction: Transaction) -> bool:
        return transaction.payment_format == self.payment_method
