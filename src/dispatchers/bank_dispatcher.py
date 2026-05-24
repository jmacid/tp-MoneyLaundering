import json
import os
from typing import Any
from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from domain.message_type import MessageType
import hashlib

class BankDispatcher:
    def __init__(self):
        outputs = os.getenv("OUTPUTS", "")
        if not outputs:
            raise ValueError("Missing OUTPUTS")
        
        self.shards = [s.strip() for s in outputs.split(",") if s.strip()]
        self.num_shards = len(self.shards)
        
        self.middlewares = {
            queue: MessageMiddlewareQueueRabbitMQ(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                queue_name=queue,
            )
            for queue in self.shards
        }

    def _get_shard(self, bank: str) -> str:
        index = int(hashlib.md5(bank.encode()).hexdigest(), 16) % self.num_shards
        return self.shards[index]

    def process(self, transaction: dict[str, Any]) -> None:
        shard_queue = self._get_shard(transaction["to_bank"])
        message = {"type": MessageType.TRANSACTION, **transaction}
        self.middlewares[shard_queue].send(json.dumps(message))