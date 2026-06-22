import json
import os
import logging

from shared.coordinator_client.coordinator_client import CoordinatorClient
from workers.consumers.exchange_consumer import ExchangeConsumer
from workers.consumers.queue_consumer import QueueConsumer
from workers.dispatchers.exchange_dispatcher import ExchangeDispatcher
from workers.dispatchers.projection_dispatcher import ProjectionDispatcher
from workers.dispatchers.queue_dispatcher import QueueDispatcher
from workers.dispatchers.sharding_dispatcher import ShardingDispatcher
from operations.core.operation_factory import OperationFactory
from workers.dispatchers.broadcast_dispatcher import BroadcastDispatcher
from workers.dispatchers.bank_dispatcher import BankDispatcher


ALLOWED_OPERATIONS = [
    "currency_filter",
    "amount_filter",
    "date_range_filter",
    "payment_method_filter",
    "payment_method_counter",
    "currency_normalizer",
    "projection_dispatcher",
    "bank_dispatcher",
    "destination_filter",
    "scatter_gather_detector",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")


def build_operation():
    operation_type = os.getenv("OPERATION_TYPE")

    if operation_type is None:
        raise ValueError("Missing environment variable: OPERATION_TYPE")

    if operation_type not in ALLOWED_OPERATIONS:
        raise ValueError(f"Unsupported operation type: {operation_type}")

    if operation_type == "projection_dispatcher":
        return ProjectionDispatcher()

    if operation_type == "bank_dispatcher":
        return BankDispatcher()

    return OperationFactory.create(operation_type)


def initialize_dispatcher():
    middleware_type = os.getenv("OUTPUT_MIDDLEWARE_TYPE", "queue")

    if middleware_type == "none":
        return None

    if middleware_type == "queue":
        return QueueDispatcher()

    if middleware_type == "exchange":
        return ExchangeDispatcher()

    if middleware_type == "sharding_exchange":
        return ShardingDispatcher()

    if middleware_type == "broadcast":
        return BroadcastDispatcher()

    raise ValueError(f"Unsupported OUTPUT_MIDDLEWARE_TYPE: {middleware_type}")


def initialize_consumer():
    middleware_type = os.getenv("INPUT_MIDDLEWARE_TYPE", "queue")

    if middleware_type == "queue":
        return QueueConsumer()

    if middleware_type == "exchange":
        return ExchangeConsumer()

    raise ValueError(f"Unsupported INPUT_MIDDLEWARE_TYPE: {middleware_type}")


def is_eof_message(message) -> bool:
    """
    Adaptar según el formato real de EOF que estés usando.

    Ejemplos soportados:
    - {"type": "EOF", "client_id": "..."}
    - {"event": "EOF", "client_id": "..."}
    - ["client_1", "EOF"]
    """

    if isinstance(message, dict):
        return message.get("type") == "EOF" or message.get("event") == "EOF"

    if isinstance(message, list):
        return len(message) >= 2 and message[1] == "EOF"

    return False


def get_client_id(message) -> str:
    """
    Adaptar según tu formato real de transacción.

    Por lo que tenías antes:
    - si es list, client_id = transaction[0]
    - si es dict, client_id = transaction["client_id"]
    """

    if isinstance(message, list):
        return message[0]

    if isinstance(message, dict):
        client_id = message.get("client_id")

        if client_id is None:
            raise ValueError(f"Missing client_id in message: {message}")

        return client_id

    raise ValueError(f"Unsupported message format: {message}")


def get_node_id() -> str:

    return (
        os.getenv("NODE_ID")
        or os.getenv("NODE_NAME")
        or os.getenv("OPERATION_TYPE")
    )


def main():
    operation = build_operation()
    dispatcher = initialize_dispatcher()
    consumer = initialize_consumer()

    operation_type = os.getenv("OPERATION_TYPE")

    node_id = get_node_id()
    rule_id = os.getenv("RULE_ID")
    stage_id = os.getenv("STAGE_ID", operation_type)
    next_stage_id = os.getenv("NEXT_STAGE_ID")

    if node_id is None:
        raise ValueError("Missing NODE_ID/NODE_NAME/OPERATION_TYPE")

    if rule_id is None:
        raise ValueError("Missing environment variable: RULE_ID")

    def on_release_client(client_id: str) -> None:
        if dispatcher is None:
            return
        eof_marker = json.dumps({"event": "EOF", "client_id": client_id})
        dispatcher.send_raw(eof_marker.encode("utf-8"))
        logging.info("Forwarded EOF downstream. client_id=%s node_id=%s stage_id=%s", client_id, node_id, stage_id)

    coordinator = CoordinatorClient(
        node_id=node_id,
        rule_id=rule_id,
        stage_id=stage_id,
        next_stage_id=next_stage_id,
        rabbitmq_host=RABBITMQ_HOST,
        on_release_client=on_release_client,
    )

    coordinator.start()

    registered = coordinator.wait_until_registered(timeout=10)

    if not registered:
        raise RuntimeError("Coordinator did not send WELCOME. Worker will not start processing.")

    logging.info(
        "Initialized worker successfully. operation=%s node_id=%s rule_id=%s stage_id=%s next_stage_id=%s",
        operation_type,
        node_id,
        rule_id,
        stage_id,
        next_stage_id,
    )

    def handle_message(message):
        client_id = get_client_id(message)

        if is_eof_message(message):
            logging.info(
                "EOF detected. client_id=%s node_id=%s stage_id=%s",
                client_id,
                node_id,
                stage_id,
            )

            coordinator.notify_eof_detected(client_id)
            return

        coordinator.record_processed(client_id)

        result = operation.process(message)

        if result is not None:
            logging.info("Processed transaction result: %s", result)

        emitted_count = 0

        if result is not None and dispatcher is not None:
            dispatcher.process([result])
            emitted_count = 1

        elif isinstance(operation, ProjectionDispatcher):
            # Ojo: este caso depende de cómo esté implementado ProjectionDispatcher.
            # Si ProjectionDispatcher ya despacha internamente y no devuelve result,
            # entonces el emitted debería salir de la propia operación.
            emitted_count = 1

        if emitted_count > 0:
            coordinator.record_emitted(client_id, emitted_count)

    try:
        consumer.start(handle_message)

    finally:
        coordinator.stop()


if __name__ == "__main__":
    main()