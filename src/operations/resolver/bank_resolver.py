import json
import os
import logging
from typing import Any
from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from domain.message_type import MessageType

class BankResolver:
    def __init__(self):
        self.output_middleware = MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=os.getenv("OUTPUT_QUEUE"),
        )
        # client_id → {bank_id → bank_name}
        self.bank_names: dict[str, dict[str, str]] = {}

    def load_bank_name(self, message: dict[str, Any]) -> None:
        client_id = message.get("client_id")
        bank_id = message.get("bank_id")
        bank_name = message.get("bank_name")

        if client_id not in self.bank_names:
            self.bank_names[client_id] = {}

        if bank_id and bank_name:
            self.bank_names[client_id][bank_id] = bank_name

    def process(self, transaction: dict[str, Any]) -> None:
        client_id = transaction.get("client_id")
        bank_id = transaction.get("from_bank") 
        bank_name = self.bank_names.get(client_id, {}).get(bank_id)

        if not bank_name:
            raise ValueError(f"Bank name not found for id: {bank_id} client: {client_id}")

        result = {
            "type": MessageType.TRANSACTION,
            "client_id": client_id,
            "bank_name": bank_name,
            "from_account": transaction.get("from_account"),
            "max_amount": transaction.get("max_amount")
        }
        self.output_middleware.send(json.dumps(result))

    def flush(self, client_id: str) -> None:
        # Limpiar estado del cliente
        if client_id in self.bank_names:
            del self.bank_names[client_id]
            logging.info(f"Cleaned bank_names state for client {client_id}")

    def handle(self, message: dict[str, Any]) -> None:
        msg_type = message.get("type")

        if msg_type == MessageType.BANK_NAME:
            self.load_bank_name(message)
        elif msg_type == MessageType.TRANSACTION:
            self.process(message)
        elif msg_type == MessageType.EOF:
            pass  # manejado por el worker
        else:
            raise ValueError(f"Unknown message type: {msg_type}")