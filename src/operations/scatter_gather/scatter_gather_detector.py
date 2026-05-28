from collections import defaultdict
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator

class ScatterGatherDetector(OperationStrategy, TransactionValidator):

    def __init__(self) -> None:

        self.required_fields = {"client_id", "from_account", "to_account"}
        self.account_flow: dict[str, dict[str, set[str]]] = defaultdict(
            lambda: {
                "incoming": set(),
                "outgoing": set(),
            }
        )

    def register_transaction(self, transaction: dict[str, Any]) -> None:

        from_account = transaction["from_account"]
        to_account = transaction["to_account"]

        self.account_flow[from_account]["outgoing"].add(to_account)
        self.account_flow[to_account]["incoming"].add(from_account)

    def process(self, transaction: dict[str, Any]) -> None:
        TransactionValidator.validate_required_fields(transaction, self.required_fields)
        self.register_transaction(transaction)
