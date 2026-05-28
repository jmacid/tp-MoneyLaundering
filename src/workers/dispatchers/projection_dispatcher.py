import json
from typing import Any
from workers.dispatchers.queue_dispatcher import QueueDispatcher
from operations.projectors.field_projector import FieldProjector
from domain.message_type import MessageType

Q_1 = ["client_id", "timestamp", "from_account", "to_account", "amount_paid"]
Q_2 = ["client_id", "from_account", "from_bank", "amount_paid", "payment_currency", "receiving_currency"]
Q_3 = ["client_id", "timestamp", "from_account", "payment_format", "amount_paid"]
Q_4 = ["client_id", "timestamp", "from_account", "to_account"]
Q_5 = ["client_id", "timestamp", "payment_format", "amount_paid", "payment_currency"]

QUERY_IDS = ["query_1", "query_2", "query_3", "query_4", "query_5"]

class ProjectionDispatcher:
    def __init__(self):
        self.dispatcher = QueueDispatcher()
        self.projectors = [
            FieldProjector(Q_1),
            FieldProjector(Q_2),
            FieldProjector(Q_3),
            FieldProjector(Q_4),
            FieldProjector(Q_5),
        ]
        # output_queues mantiene el orden de OUTPUTS del compose
        self.output_queues = self.dispatcher.output_queues

    def process(self, transaction: dict[str, Any]) -> None:
        projected = [p.process(transaction) for p in self.projectors]
        self.dispatcher.process(projected)

    def flush(self, client_id: str) -> None:
        for query_id, queue in zip(QUERY_IDS, self.output_queues):
            eof_message = json.dumps({
                "type": MessageType.EOF,
                "client_id": client_id,
                "query_id": query_id,
            }).encode()
            self.dispatcher.middlewares[queue].send(eof_message)