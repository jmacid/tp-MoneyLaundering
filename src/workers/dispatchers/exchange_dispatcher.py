from typing import Any

from common import middleware


class ExchangeDispatcher:

    def __init__(self,exchange_name: str):

        self.middleware = (
            middleware.MessageMiddlewareExchangeRabbitMQ(
                host="rabbitmq",
                exchange_name=exchange_name,
            )
        )

    def process(self, transaction: dict[str, Any], routing_key: str) -> None:

        self.middleware.publish(
            routing_key=routing_key,
            body=transaction,
        )