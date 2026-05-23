from abc import ABC, abstractmethod
from domain.transaction import Transaction

class OperationStrategy(ABC):

    @abstractmethod
    def process(self, transaction: Transaction) -> bool:
        pass
