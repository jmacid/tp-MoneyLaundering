import json
import os

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

class QueuePublisher:

    def __init__(self):
        self.host = os.getenv("RABBITMQ_HOST", "rabbitmq")

    def publish(self, queue_name: str, message: dict) -> None:
        MessageMiddlewareQueueRabbitMQ(
            host=self.host,
            queue_name=queue_name,
        ).send(json.dumps(message))