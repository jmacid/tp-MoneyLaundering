import json
import os
from typing import Any

from common import middleware


class QueueDispatcher:

    def __init__(self):
        outputs = os.getenv("OUTPUTS", "")

        if not outputs:
            raise ValueError("Missing OUTPUTS")

        self.output_queues = [
            queue.strip()
            for queue in outputs.split(",")
            if queue.strip()
        ]

        self.expected_transactions = len(self.output_queues)

        self.middlewares = {
            queue: middleware.MessageMiddlewareQueueRabbitMQ(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                queue_name=queue,
            )
            for queue in self.output_queues
        }

    def process(self, transactions: list[dict[str, Any]]) -> None:
        if len(transactions) != self.expected_transactions:
            raise ValueError(
                "Unexpected amount of transactions in QueueDispatcher",
                f"transactions={len(transactions)} and expected={self.expected_transactions}"
            )

        for queue, transaction in zip(self.output_queues, transactions):
            self.middlewares[queue].send(json.dumps(transaction))

    def dispatch_batch(self, results: list[dict[str, Any]]) -> None:
        queue = self.output_queues[0]
        self.middlewares[queue].send(json.dumps(results))

    def dispatch_fan_out_batch(self, batches_per_queue: list[list[dict[str, Any]]]) -> None:
        for queue, batch in zip(self.output_queues, batches_per_queue):
            self.middlewares[queue].send(json.dumps(batch))