from collections import defaultdict
import logging
from typing import Any
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator

class PaymentMethodCounter(OperationStrategy):

    def __init__(self):

        self.count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.required_fields = {"payment_format"} #"client_id", only for demo

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:

        TransactionValidator.validate_required_fields(transaction, self.required_fields)

        client_id = "Hardcoded for demo" # transaction["client_id"] for DEMO
        payment_method = transaction["payment_format"]

        logging.info(f"Processing Client={client_id} Payment={payment_method}")

        if client_id not in self.count:
            self.count[client_id] = {}

        if payment_method not in self.count[client_id]:
            self.count[client_id][payment_method] = 0

        self.count[client_id][payment_method] += 1

        logging.info(f"Client={client_id} Payment={payment_method} Count {self.count[client_id][payment_method]}")        
