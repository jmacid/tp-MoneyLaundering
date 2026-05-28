import json
import os
from typing import Any
from common import middleware

class BroadcastDispatcher:

    def __init__(self):
        outputs = os.getenv("OUTPUTS", "")

        if not outputs:
            raise ValueError("Missing OUTPUTS")

        self.output_queues = [
            queue.strip()
            for queue in outputs.split(",")
            if queue.strip()
        ]

        self.middlewares = {
            queue: middleware.MessageMiddlewareQueueRabbitMQ(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                queue_name=queue,
            )
            for queue in self.output_queues
        }

    def process(self, transactions: list[dict[str, Any]]) -> None:
        for transaction in transactions:
            for queue in self.output_queues:
                self.middlewares[queue].send(json.dumps(transaction))