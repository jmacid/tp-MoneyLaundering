import json
import os
import time
from typing import Any

import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
COORDINATOR_QUEUE = os.getenv("COORDINATOR_QUEUE", "coordinator_control_queue")

CLIENT_ID = "client_1"
RULE_ID = "1"
STAGE_ID = "currency_filter"

NODE_1_ID = "currency_filter_1"
NODE_2_ID = "currency_filter_2"

NODE_1_CONTROL_QUEUE = f"{NODE_1_ID}_control_queue"
NODE_2_CONTROL_QUEUE = f"{NODE_2_ID}_control_queue"


def pause(message: str = "Press ENTER to continue...") -> None:
    input(f"\n>>> {message}")


def connect() -> tuple[pika.BlockingConnection, pika.adapters.blocking_connection.BlockingChannel]:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    return connection, channel


def declare_queue(channel, queue_name: str) -> None:
    channel.queue_declare(
        queue=queue_name,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )


def purge_queue(channel, queue_name: str) -> None:
    channel.queue_purge(queue=queue_name)


def publish(channel, queue_name: str, message: dict[str, Any]) -> None:
    body = json.dumps(message).encode("utf-8")

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )

    print(f"[SEND] queue={queue_name} message={message}")


def read_one(channel, queue_name: str, timeout_seconds: int = 10) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        method, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)

        if method is not None:
            message = json.loads(body.decode("utf-8"))
            print(f"[RECV] queue={queue_name} message={message}")
            return message

        time.sleep(0.25)

    raise TimeoutError(f"Timeout waiting message from queue={queue_name}")


def read_until_event(channel, queue_name: str, expected_event: str, timeout_seconds: int = 10) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            message = read_one(channel, queue_name, timeout_seconds=1)
        except TimeoutError:
            continue

        message_event = message.get("event") or message.get("type")

        if message_event == expected_event:
            return message

        print(f"[SKIP] queue={queue_name} unexpected_event={message_event}")

    raise TimeoutError(f"Timeout waiting event={expected_event} from queue={queue_name}")


def main() -> None:
    connection, channel = connect()

    try:
        queues = [
            COORDINATOR_QUEUE,
            NODE_1_CONTROL_QUEUE,
            NODE_2_CONTROL_QUEUE,
        ]

        print("\n--- SETUP: Declare and purge queues ---")
        pause("Press ENTER to declare and purge queues...")

        for queue in queues:
            declare_queue(channel, queue)
            purge_queue(channel, queue)

        print("[SETUP OK] Queues declared and purged")

        print("\n--- STEP 1: HELLO nodes ---")
        pause("Press ENTER to send HELLO from both nodes...")

        publish(channel, COORDINATOR_QUEUE, {
            "event": "HELLO",
            "node_id": NODE_1_ID,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
            "next_stage_id": "amount_filter",
            "control_queue": NODE_1_CONTROL_QUEUE,
        })

        publish(channel, COORDINATOR_QUEUE, {
            "event": "HELLO",
            "node_id": NODE_2_ID,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
            "next_stage_id": "amount_filter",
            "control_queue": NODE_2_CONTROL_QUEUE,
        })

        pause("Press ENTER to read WELCOME messages...")

        welcome_1 = read_until_event(channel, NODE_1_CONTROL_QUEUE, "WELCOME")
        welcome_2 = read_until_event(channel, NODE_2_CONTROL_QUEUE, "WELCOME")

        assert (welcome_1.get("event") or welcome_1.get("type")) == "WELCOME"
        assert (welcome_2.get("event") or welcome_2.get("type")) == "WELCOME"

        print("[ASSERT OK] Both nodes received WELCOME")

        print("\n--- STEP 2: Gateway sends CLIENT_INPUT_COMPLETED ---")
        pause("Press ENTER to send CLIENT_INPUT_COMPLETED...")

        publish(channel, COORDINATOR_QUEUE, {
            "event": "CLIENT_INPUT_COMPLETED",
            "client_id": CLIENT_ID,
            "expected_input": 10,
        })

        print("[INFO] Gateway input completed sent")

        print("\n--- STEP 3: Worker detects EOF for rule/stage ---")
        pause("Press ENTER to send STAGE_EOF_DETECTED...")

        publish(channel, COORDINATOR_QUEUE, {
            "event": "STAGE_EOF_DETECTED",
            "client_id": CLIENT_ID,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
        })

        print("\n--- STEP 4: Coordinator sends EOF_REPORT_REQUEST to both nodes ---")
        pause("Press ENTER to read EOF_REPORT_REQUEST from both node queues...")

        request_1 = read_until_event(channel, NODE_1_CONTROL_QUEUE, "EOF_REPORT_REQUEST")
        request_2 = read_until_event(channel, NODE_2_CONTROL_QUEUE, "EOF_REPORT_REQUEST")

        assert request_1["request_id"] == request_2["request_id"]
        assert request_1["client_id"] == CLIENT_ID
        assert request_2["client_id"] == CLIENT_ID

        request_id = request_1["request_id"]

        print(f"[ASSERT OK] Same request_id received by both nodes: {request_id}")

        print("\n--- STEP 5A: Node 1 sends EOF_REPORT ---")
        pause("Press ENTER to send EOF_REPORT from node 1...")

        publish(channel, COORDINATOR_QUEUE, {
            "event": "EOF_REPORT",
            "request_id": request_id,
            "node_id": NODE_1_ID,
            "client_id": CLIENT_ID,
            "processed": 6,
            "emitted": 3,
        })

        print("[INFO] At this point coordinator should still be WAITING for node 2")

        print("\n--- STEP 5B: Node 2 sends EOF_REPORT ---")
        pause("Press ENTER to send EOF_REPORT from node 2...")

        publish(channel, COORDINATOR_QUEUE, {
            "event": "EOF_REPORT",
            "request_id": request_id,
            "node_id": NODE_2_ID,
            "client_id": CLIENT_ID,
            "processed": 4,
            "emitted": 4,
        })

        print("[INFO] Now coordinator should close request and release client")

        print("\n--- STEP 6: Coordinator should release client ---")
        pause("Press ENTER to read RELEASE_CLIENT from both node queues...")

        release_1 = read_until_event(channel, NODE_1_CONTROL_QUEUE, "RELEASE_CLIENT")
        release_2 = read_until_event(channel, NODE_2_CONTROL_QUEUE, "RELEASE_CLIENT")

        assert release_1["client_id"] == CLIENT_ID
        assert release_2["client_id"] == CLIENT_ID

        if "request_id" in release_1:
            assert release_1["request_id"] == request_id

        if "request_id" in release_2:
            assert release_2["request_id"] == request_id

        print("\n[TEST OK] EOF coordination round completed successfully")
        print(f"[TEST OK] request_id={request_id}")
        print("[TEST OK] processed total = 10")
        print("[TEST OK] emitted total = 7")

    finally:
        connection.close()


if __name__ == "__main__":
    main()