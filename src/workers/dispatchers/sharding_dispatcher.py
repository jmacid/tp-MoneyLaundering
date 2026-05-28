import json
import os
import hashlib
from typing import Any

from common import middleware

class ShardingDispatcher:

    def __init__(self):
        self.exchange_name = os.getenv("OUTPUT_EXCHANGE")
        if not self.exchange_name:
            raise ValueError("Missing OUTPUT_EXCHANGE")

        shards_count_raw = os.getenv("SHARDS_COUNT")
        if not shards_count_raw:
            raise ValueError("Missing SHARDS_COUNT")
        self.shards_count = int(shards_count_raw)

        self.sharding_key_field = os.getenv("SHARDING_KEY_FIELD")
        if not self.sharding_key_field:
            raise ValueError("Missing SHARDING_KEY_FIELD")

        self.middlewares = []
        for i in range(self.shards_count):
            routing_key = f"{self.exchange_name}_{i}"
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                exchange_name=self.exchange_name,
                routing_keys=[routing_key]
            )
            self.middlewares.append(exchange)

    def process(self, transactions: list[dict[str, Any]]) -> None:
        for transaction in transactions:
            key_value = transaction.get(self.sharding_key_field, "")
            
            hash_val = int(hashlib.md5(str(key_value).encode('utf-8')).hexdigest(), 16)
            shard_id = hash_val % self.shards_count
            
            self.middlewares[shard_id].send(json.dumps(transaction))