import os
from typing import Any

from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

class QueueDispatcher():

    def __init__(self):
        self.outputs = os.getenv("OUTPUTS", "")

        if not self.outputs:
            raise ValueError("Missing OUTPUTS")
        
        self.output_queues = [
            queue.strip()
            for queue in self.outputs.split(",")
            if queue.strip()
        ]

        self.expected_transactions = len(self.output_queues)
        self.middleware = MessageMiddlewareQueueRabbitMQ(
            host="rabbitmq",
            queues=self.output_queues,
        )

    def process(self, transactions: list[dict[str, Any]]) -> None:
        if len(transactions) != self.expected_transactions:
            raise ValueError("Unexpected amount of transactions in Dispatcher")
        
        for queue, transaction in zip(
            self.output_queues,
            transactions,
        ):

            self.middleware.publish(
                queue=queue,
                body=transaction,
            )