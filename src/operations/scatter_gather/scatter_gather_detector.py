from collections import defaultdict
from typing import Any, Callable
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator


class ScatterGatherDetector(OperationStrategy, TransactionValidator):

    def __init__(self) -> None:
        self.required_fields = {"client_id", "from_account", "to_account"}
        # per-client account flow: client_id -> account -> {incoming, outgoing}
        self.account_flow: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(
            lambda: defaultdict(lambda: {"incoming": set(), "outgoing": set()})
        )

    def process(self, transaction: dict[str, Any]) -> None:
        TransactionValidator.validate_required_fields(transaction, self.required_fields)

        client_id = transaction["client_id"]
        from_account = transaction["from_account"]
        to_account = transaction["to_account"]

        self.account_flow[client_id][from_account]["outgoing"].add(to_account)
        self.account_flow[client_id][to_account]["incoming"].add(from_account)

    def flush(self, client_id: str) -> list[dict[str, Any]]:
        """Return scatter-gather paths for client_id and clear its state."""
        client_flow = self.account_flow.pop(client_id, {})
        paths = []

        for account, connections in client_flow.items():
            if connections["incoming"] and connections["outgoing"]:
                paths.append({
                    "bridge_account": account,
                    "origins": list(connections["incoming"]),
                    "destinations": list(connections["outgoing"]),
                })

        if not paths:
            return []

        return [{
            "client_id": client_id,
            "query": "query_4",
            "scatter_gather_paths": paths,
        }]
