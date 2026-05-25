from typing import Any

from middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
)


class ExchangeDispatcher:

    def __init__(self,exchange_name: str):

        self.middleware = (
            MessageMiddlewareExchangeRabbitMQ(
                host="rabbitmq",
                exchange_name=exchange_name,
            )
        )

    def process(self, transaction: dict[str, Any], routing_key: str) -> None:

        self.middleware.publish(
            routing_key=routing_key,
            body=transaction,
        )