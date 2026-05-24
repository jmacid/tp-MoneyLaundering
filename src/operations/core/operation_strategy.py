from abc import ABC, abstractmethod
from typing import Any

class OperationStrategy(ABC):

    @abstractmethod
    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        pass
