import os
from typing import Any
from domain.message_type import MessageType
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ


class ExchangeDispatcher:

    def __init__(self,exchange_name: str):

        self.middleware = (
            MessageMiddlewareExchangeRabbitMQ(
                host= os.getenv("RABBITMQ_HOST", "rabbitmq"),
                exchange_name=exchange_name,
            )
        )

    def process(self, transaction: dict[str, Any], routing_key: str) -> None:
        message = {"type": MessageType.TRANSACTION, **transaction}
        self.middleware.publish(
            routing_key=routing_key,
            body=message,
        )