from typing import Any
from workers.dispatchers.queue_dispatcher import QueueDispatcher
from operations.projectors.field_projector import FieldProjector

Q_1 = ["client_id", "timestamp", "from_account","to_account","amount_paid", "receiving_currency", "payment_currency"]
Q_2 = ["client_id", "from_account", "to_bank", "amount_paid", "payment_currency", "receiving_currency"]
Q_3 = ["client_id", "timestamp","from_account","payment_format","amount_paid", "payment_currency", "receiving_currency"]
Q_4 = ["client_id", "from_account", "to_account", "payment_currency", "receiving_currency", "timestamp"]
Q_5 = ["client_id", "timestamp","payment_format","amount_paid","payment_currency", "amount_received", "receiving_currency"]


class ProjectionDispatcher():

    def __init__(self):
        self.dispatcher = QueueDispatcher()

        projector_q1 = FieldProjector(Q_1)
        projector_q2 = FieldProjector(Q_2)
        projector_q3 = FieldProjector(Q_3)
        projector_q4 = FieldProjector(Q_4)
        projector_q5 = FieldProjector(Q_5)

        self.projectors = [projector_q1, projector_q2, projector_q3, projector_q4, projector_q5]

    def process(self, transaction: dict[str, Any]) -> None:
        projected_transactions: list[dict[str, Any]] = []
        for projector in self.projectors:
            projected_transactions.append(projector.process(transaction))
        self.dispatcher.process(projected_transactions)

    def process_batch(self, transactions: list[dict[str, Any]]) -> None:
        batches_per_queue = [[] for _ in self.projectors]
        for transaction in transactions:
            for i, projector in enumerate(self.projectors):
                batches_per_queue[i].append(projector.process(transaction))
        self.dispatcher.dispatch_fan_out_batch(batches_per_queue)