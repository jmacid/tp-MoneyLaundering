from typing import Any
from dispatchers.queue_dispatcher import QueueDispatcher
from operations.projectors.field_projector import FieldProjector

Q_1 = ["timestamp", "from_account","to_account","amount_paid"]
Q_2 = ["from_account","to_bank","amount_paid"]
Q_3 = ["timestamp","from_account","payment_format","amount_paid"]
Q_4 = ["timestamp","from_account","to_account"]
Q_5 = ["timestamp","payment_format","amount_paid","payment_currency"]


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
            projected_transaction = projector.process(transaction)
            projected_transactions.append(projected_transaction)

        self.dispatcher.process(projected_transactions)