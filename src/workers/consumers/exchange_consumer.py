import json
import logging
import os
from typing import Any

from common import middleware


class ExchangeConsumer:

    def __init__(self):
        exchange_name = os.getenv("INPUT_EXCHANGE")
        routing_keys_raw = os.getenv("INPUT_ROUTING_KEYS", "")

        if not exchange_name:
            raise ValueError("Missing INPUT_EXCHANGE")

        if not routing_keys_raw:
            raise ValueError("Missing INPUT_ROUTING_KEYS")

        routing_keys = [
            key.strip()
            for key in routing_keys_raw.split(",")
            if key.strip()
        ]

        self.middleware = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            exchange_name=exchange_name,
            routing_keys=routing_keys,
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