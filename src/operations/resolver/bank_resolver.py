import json
import os
from typing import Any
from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from domain.message_type import MessageType

class BankResolver:
    def __init__(self):
        self.output_middleware = MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=os.getenv("OUTPUT_QUEUE"),
        )
        self.bank_names = {}  

    def load_bank_name(self, message: dict[str, Any]) -> None:
        bank_id = message.get("bank_id")
        bank_name = message.get("bank_name")
        if bank_id and bank_name:
            self.bank_names[bank_id] = bank_name

    def process(self, transaction: dict[str, Any]) -> None:
        bank_id = transaction.get("to_bank")
        bank_name = self.bank_names.get(bank_id)

        if not bank_name:
            raise ValueError(f"Bank name not found for id: {bank_id}")

        result = {
            "type": MessageType.TRANSACTION,
            "bank_name": bank_name,
            "from_account": transaction.get("from_account"),
            "max_amount": transaction.get("max_amount")
        }
        self.output_middleware.send(json.dumps(result))

    def handle(self, message: dict[str, Any]) -> None:
        msg_type = message.get("type")

        if msg_type == MessageType.BANK_NAME:
            self.load_bank_name(message)
        elif msg_type == MessageType.TRANSACTION:
            self.process(message)
        elif msg_type == MessageType.EOF:
            pass  # TODO: EOF
        else:
            raise ValueError(f"Unknown message type: {msg_type}")