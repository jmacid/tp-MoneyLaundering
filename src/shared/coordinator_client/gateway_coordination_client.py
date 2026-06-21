import json

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ


class GatewayCoordinatorClient:
    def __init__(self, rabbitmq_host: str, coordinator_queue: str):
        self._queue = MessageMiddlewareQueueRabbitMQ(
            host=rabbitmq_host,
            queue_name=coordinator_queue,
        )

    def notify_client_input_completed(self, client_id: str, expected_input: int) -> None:
        event = {
            "event": "CLIENT_INPUT_COMPLETED",
            "client_id": client_id,
            "expected_input": expected_input,
        }

        self._queue.send(json.dumps(event).encode("utf-8"))

    def close(self) -> None:
        self._queue.close()