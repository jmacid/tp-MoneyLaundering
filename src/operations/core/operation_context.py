from typing import Any
from domain.transaction import Transaction
from operations.core import operation_strategy

class OperationContext:

    def __init__(self, strategy: operation_strategy):
        self._strategy = strategy

    def set_strategy(self, strategy: operation_strategy) -> None:
        self._strategy = strategy

    def execute(self, transaction: dict[str, Any]) -> bool:
        return self._strategy.process(transaction)
