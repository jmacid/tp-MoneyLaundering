import json
import os
from typing import Any
from domain.message_type import MessageType
from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

class LocalBankMaxAggregator:
    def __init__(self):
        self.middleware = MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=os.getenv("OUTPUT_QUEUE"),
        )
        # client_id → {bank → {amount, account}}
        self.max_amounts: dict[str, dict] = {} 


    def update_max(self, transaction: dict) -> None:
        client_id = transaction.get("client_id")
        bank = transaction.get("to_bank")
        amount = transaction.get("amount_paid", 0)

        if client_id not in self.max_amounts:
            self.max_amounts[client_id] = {}

        if bank not in self.max_amounts[client_id] or amount > self.max_amounts[client_id][bank]["amount"]:
            self.max_amounts[client_id][bank] = {
                "amount": amount,
                "from_account": transaction.get("from_account")
            }

    def process(self, transaction: dict[str, Any]) -> None:
        self.update_max(transaction)

    def flush(self, client_id: str) -> None:
        if client_id not in self.max_amounts:
            return
        for bank, data in self.max_amounts[client_id].items():
            result = {
                "type": MessageType.TRANSACTION,
                "client_id": client_id,
                "to_bank": bank,
                "from_account": data["from_account"],
                "max_amount": data["amount"]
            }
            self.middleware.send(json.dumps(result))
        # Limpiar estado del cliente
        del self.max_amounts[client_id]