import json
import os
import time
from typing import Any

import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
COORDINATOR_QUEUE = os.getenv("COORDINATOR_QUEUE", "coordinator_control_queue")

QUEUE_ARGUMENTS = {"x-queue-type": "quorum"}

CLIENT_ID = "client_1"
RULE_ID = "q1"

STAGE_ID = "currency_filter"
NEXT_STAGE_ID = "amount_filter"

NODE_1 = "currency_filter_1"
NODE_2 = "currency_filter_2"

NODE_1_CONTROL_QUEUE = f"{NODE_1}_control_queue"
NODE_2_CONTROL_QUEUE = f"{NODE_2}_control_queue"


def connect() -> tuple[pika.BlockingConnection, pika.adapters.blocking_connection.BlockingChannel]:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    return connection, channel


def declare_queue(channel, queue_name: str) -> None:
    channel.queue_declare(
        queue=queue_name,
        durable=True,
        arguments=QUEUE_ARGUMENTS,
    )


def publish(channel, queue_name: str, message: dict[str, Any]) -> None:
    declare_queue(channel, queue_name)

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(message).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )

    print(f"\n[SENT] queue={queue_name}")
    print(json.dumps(message, indent=2))


def get_message(
    channel,
    queue_name: str,
    timeout_seconds: int = 10,
) -> dict[str, Any] | None:
    declare_queue(channel, queue_name)

    started_at = time.time()

    while time.time() - started_at < timeout_seconds:
        method_frame, properties, body = channel.basic_get(
            queue=queue_name,
            auto_ack=True,
        )

        if method_frame:
            message = json.loads(body.decode("utf-8"))

            print(f"\n[RECEIVED] queue={queue_name}")
            print(json.dumps(message, indent=2))

            return message

        time.sleep(1)

    print(f"\n[TIMEOUT] No message received from queue={queue_name}")
    return None


def wait_for_event(
    channel,
    queue_name: str,
    expected_event: str,
    timeout_seconds: int = 10,
) -> dict[str, Any] | None:
    started_at = time.time()

    while time.time() - started_at < timeout_seconds:
        message = get_message(channel, queue_name, timeout_seconds=1)

        if not message:
            continue

        if message.get("event") == expected_event:
            return message

        print(
            f"\n[SKIPPED] Expected event={expected_event}, "
            f"but received event={message.get('event')}"
        )

    print(f"\n[TIMEOUT] No event={expected_event} received from queue={queue_name}")
    return None


def send_hello_messages(channel) -> None:
    publish(
        channel,
        COORDINATOR_QUEUE,
        {
            "event": "HELLO",
            "node_id": NODE_1,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
            "next_stage_id": NEXT_STAGE_ID,
            "control_queue": NODE_1_CONTROL_QUEUE,
        },
    )

    publish(
        channel,
        COORDINATOR_QUEUE,
        {
            "event": "HELLO",
            "node_id": NODE_2,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
            "next_stage_id": NEXT_STAGE_ID,
            "control_queue": NODE_2_CONTROL_QUEUE,
        },
    )


def read_welcome_messages(channel) -> None:
    wait_for_event(channel, NODE_1_CONTROL_QUEUE, "WELCOME")
    wait_for_event(channel, NODE_2_CONTROL_QUEUE, "WELCOME")


def send_initial_eof(channel) -> None:
    publish(
        channel,
        COORDINATOR_QUEUE,
        {
            "event": "INITIAL_EOF",
            "client_id": CLIENT_ID,
            "rule_id": RULE_ID,
            "to_stage_id": STAGE_ID,
            "expected_input": 10,
        },
    )


def send_eof_detected(channel) -> None:
    publish(
        channel,
        COORDINATOR_QUEUE,
        {
            "event": "EOF_DETECTED",
            "client_id": CLIENT_ID,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
            "node_id": NODE_1,
        },
    )


def read_request_id(channel) -> str:
    request_1 = wait_for_event(
        channel,
        NODE_1_CONTROL_QUEUE,
        "REQUEST_EOF_REPORT",
    )
    request_2 = wait_for_event(
        channel,
        NODE_2_CONTROL_QUEUE,
        "REQUEST_EOF_REPORT",
    )

    if not request_1:
        raise RuntimeError(f"No REQUEST_EOF_REPORT received in {NODE_1_CONTROL_QUEUE}")

    if not request_2:
        raise RuntimeError(f"No REQUEST_EOF_REPORT received in {NODE_2_CONTROL_QUEUE}")

    request_id_1 = request_1["request_id"]
    request_id_2 = request_2["request_id"]

    if request_id_1 != request_id_2:
        raise RuntimeError(
            f"Different request_id values received: {request_id_1} != {request_id_2}"
        )

    return request_id_1


def send_partial_report(channel, request_id: str) -> None:
    publish(
        channel,
        COORDINATOR_QUEUE,
        {
            "event": "EOF_REPORT",
            "request_id": request_id,
            "client_id": CLIENT_ID,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
            "node_id": NODE_1,
            "processed": 6,
            "emitted": 4,
        },
    )


def send_closing_report(channel, request_id: str) -> None:
    publish(
        channel,
        COORDINATOR_QUEUE,
        {
            "event": "EOF_REPORT",
            "request_id": request_id,
            "client_id": CLIENT_ID,
            "rule_id": RULE_ID,
            "stage_id": STAGE_ID,
            "node_id": NODE_2,
            "processed": 4,
            "emitted": 3,
        },
    )


def read_release_client_messages(channel) -> None:
    release_1 = wait_for_event(
        channel,
        NODE_1_CONTROL_QUEUE,
        "RELEASE_CLIENT",
        timeout_seconds=10,
    )

    release_2 = wait_for_event(
        channel,
        NODE_2_CONTROL_QUEUE,
        "RELEASE_CLIENT",
        timeout_seconds=10,
    )

    if not release_1:
        raise RuntimeError(f"No RELEASE_CLIENT received in {NODE_1_CONTROL_QUEUE}")

    if not release_2:
        raise RuntimeError(f"No RELEASE_CLIENT received in {NODE_2_CONTROL_QUEUE}")

    assert release_1["client_id"] == CLIENT_ID
    assert release_2["client_id"] == CLIENT_ID
    assert release_1["rule_id"] == RULE_ID
    assert release_2["rule_id"] == RULE_ID
    assert release_1["stage_id"] == STAGE_ID
    assert release_2["stage_id"] == STAGE_ID

    print("\n[OK] RELEASE_CLIENT received by both current-stage nodes")


def main() -> None:
    connection, channel = connect()

    try:
        print("\n=== TEST: Pipeline Coordinator EOF flow ===")

        print("\n--- Step 1: Sending HELLO messages ---")
        send_hello_messages(channel)

        time.sleep(2)

        print("\n--- Step 2: Reading WELCOME messages ---")
        read_welcome_messages(channel)

        print("\n--- Step 3: Sending INITIAL_EOF ---")
        send_initial_eof(channel)

        time.sleep(1)

        print("\n--- Step 4: Sending EOF_DETECTED ---")
        send_eof_detected(channel)

        time.sleep(2)

        print("\n--- Step 5: Reading REQUEST_EOF_REPORT messages ---")
        request_id = read_request_id(channel)

        print(f"\n[INFO] request_id = {request_id}")

        print("\n--- Step 6: Sending partial EOF_REPORT from node 1 ---")
        send_partial_report(channel, request_id)

        print("\n[INFO] Coordinator should NOT close yet because node 2 is missing.")
        time.sleep(3)

        print("\n--- Step 7: Sending closing EOF_REPORT from node 2 ---")
        send_closing_report(channel, request_id)

        print("\n[INFO] Coordinator should close the stage now.")
        print("[INFO] Expected total_processed = 10")
        print("[INFO] Expected total_emitted = 7")
        print("[INFO] Current stage should receive RELEASE_CLIENT")

        time.sleep(2)

        print("\n--- Step 8: Reading RELEASE_CLIENT messages ---")
        read_release_client_messages(channel)

        print("\n=== TEST FINISHED OK ===")

    finally:
        connection.close()


if __name__ == "__main__":
    main()