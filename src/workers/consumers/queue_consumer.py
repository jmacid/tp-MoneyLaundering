import json
import logging
import os
from typing import Any

from common import middleware


class QueueConsumer:

    def __init__(self):
        queue_name = os.getenv("INPUT_QUEUE")

        if not queue_name:
            raise ValueError("Missing INPUT_QUEUE")

        self.middleware = middleware.MessageMiddlewareQueueRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            queue_name=queue_name,
        )

    def start(self, handler) -> None:
        def callback(body, ack, nack):
            try:
                message: dict[str, Any] = json.loads(body)

                logging.info("Message received: %s", message)

                handler(message)

                ack()

            except Exception:
                logging.exception("Error processing message")
                nack()

        self.middleware.start_consuming(callback)

    def stop(self) -> None:
        self.middleware.stop_consuming()