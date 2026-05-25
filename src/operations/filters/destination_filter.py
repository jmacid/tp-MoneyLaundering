from decimal import Decimal
import logging
from numbers import Number
import os
from typing import Any
from operations.core.operation_strategy import OperationStrategy

class DestinationFilter(OperationStrategy):

    REQUIRED_FIELDS = {
        #"client_id", only for demo
        "from_account",
        "to_account",
    }

    def __init__(self, required_accounts: Number | None = None):

        required_accounts_raw = os.getenv("DESTINATION_ACC_REQUIRED")

        if required_accounts is None and not required_accounts_raw:
            raise ValueError("Missing DESTINATION_ACC_REQUIRED")
        
        self.destination_required_acc = required_accounts or Decimal(required_accounts_raw)

        self.count = {}

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:

        self._validate_transaction(transaction)

        client_id = "Hardcoded for demo" # transaction["client_id"] for DEMO
        from_account = transaction["from_account"]
        to_account = transaction["to_account"]

        logging.info(f"Processing client <<{client_id}>> from account <<{from_account}>>")

        if client_id not in self.count:
            self.count[client_id] = {}

        if from_account not in self.count[client_id]:
            self.count[client_id][from_account] = set()

        self.count[client_id][from_account].add(to_account)

        return self._validate_condition(client_id, from_account, to_account)
    
    def _validate_condition(self, client_id: str, from_account: str) -> dict[str, Any] | None:
        if (len(self.count[client_id][from_account])) >= self.destination_required_acc:
            return {"from_account": from_account}
        return None

    def _validate_transaction(self, transaction: dict) -> None:
        missing_fields = self.REQUIRED_FIELDS - transaction.keys()

        if missing_fields:
            raise ValueError(
                f"Missing required fields: {missing_fields}"
            )        
