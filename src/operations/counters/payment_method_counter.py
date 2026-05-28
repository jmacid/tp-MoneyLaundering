import json
import logging
import os
from collections import defaultdict
from typing import Any

from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from domain.message_type import MessageType

class PaymentMethodCounter(OperationStrategy):
    def __init__(self):
        self.count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.required_fields = {"payment_format", "client_id"}
        self.output_middleware = MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=os.getenv("OUTPUTS"),
        )

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        TransactionValidator.validate_required_fields(transaction, self.required_fields)
        client_id = transaction["client_id"]
        payment_method = transaction["payment_format"]
        self.count[client_id][payment_method] += 1
        logging.info(f"Client={client_id[:8]} Payment={payment_method} Count={self.count[client_id][payment_method]}")
        return None  # no emite por transacción, solo en flush

    def flush(self, client_id: str) -> None:
        counts = self.count.get(client_id, {})
        total = sum(counts.values())
        result = json.dumps({
            "type": MessageType.TRANSACTION,
            "client_id": client_id,
            "query_id": "query_5",
            "total_minor_transactions": total,
            "breakdown": counts,
        })
        self.output_middleware.send(result)
        logging.info(f"Flushed count for client {client_id[:8]}: {total}")
        del self.count[client_id]