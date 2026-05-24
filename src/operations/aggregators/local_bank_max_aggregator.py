import json
import os
from typing import Any
from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

class LocalBankMaxAggregator:
    def __init__(self):
        self.middleware = MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=os.getenv("OUTPUT_QUEUE"),
        )
        self.max_amounts = {}

    def update_max(self, transaction: dict[str, Any]) -> None:
        bank = transaction.get("to_bank")
        amount = transaction.get("amount_paid", 0)

        if bank not in self.max_amounts or amount > self.max_amounts[bank]["amount"]:
            self.max_amounts[bank] = {
                "amount": amount,
                "from_account": transaction.get("from_account")
            }

    def process(self, transaction: dict[str, Any]) -> None:
        self.update_max(transaction)

    def flush(self) -> None:
        for bank, data in self.max_amounts.items():
            result = {
                "to_bank": bank,
                "from_account": data["from_account"],
                "max_amount": data["amount"]
            }
            self.middleware.send(json.dumps(result))