import json
import os
import threading
import time
from typing import Any

import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
COORDINATOR_QUEUE = os.getenv("COORDINATOR_QUEUE", "coordinator_control_queue")

CLIENT_ID = "client_1"
RULE_ID = "1"

STAGE_1_ID = "currency_filter"
STAGE_2_ID = "amount_filter"

CURRENCY_NODE_1_ID = "currency_filter_1"
CURRENCY_NODE_2_ID = "currency_filter_2"

AMOUNT_NODE_1_ID = "amount_filter_1"
AMOUNT_NODE_2_ID = "amount_filter_2"

CURRENCY_NODE_1_QUEUE = f"{CURRENCY_NODE_1_ID}_control_queue"
CURRENCY_NODE_2_QUEUE = f"{CURRENCY_NODE_2_ID}_control_queue"

AMOUNT_NODE_1_QUEUE = f"{AMOUNT_NODE_1_ID}_control_queue"
AMOUNT_NODE_2_QUEUE = f"{AMOUNT_NODE_2_ID}_control_queue"


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


def publish(channel, queue_name: str, message: dict[str, Any], verbose: bool = True) -> None:
    body = json.dumps(message).encode("utf-8")

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )

    if verbose:
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


def start_heartbeat_thread(node_ids: list[str], stop_event: threading.Event) -> threading.Thread:
    thread = threading.Thread(
        target=heartbeat_loop,
        args=(node_ids, stop_event),
        daemon=True,
    )
    thread.start()
    return thread


def heartbeat_loop(node_ids: list[str], stop_event: threading.Event) -> None:
    connection, channel = connect()

    try:
        declare_queue(channel, COORDINATOR_QUEUE)

        while not stop_event.is_set():
            for node_id in node_ids:
                publish(
                    channel,
                    COORDINATOR_QUEUE,
                    {
                        "event": "HEARTBEAT",
                        "node_id": node_id,
                    },
                    verbose=False,
                )

            stop_event.wait(5)

    finally:
        connection.close()


def send_hello(
    channel,
    node_id: str,
    rule_id: str,
    stage_id: str,
    next_stage_id: str | None,
    control_queue: str,
) -> None:
    message = {
        "event": "HELLO",
        "node_id": node_id,
        "rule_id": rule_id,
        "stage_id": stage_id,
        "next_stage_id": next_stage_id,
        "control_queue": control_queue,
    }

    publish(channel, COORDINATOR_QUEUE, message)


def send_stage_eof_detected(channel, stage_id: str) -> None:
    publish(channel, COORDINATOR_QUEUE, {
        "event": "STAGE_EOF_DETECTED",
        "client_id": CLIENT_ID,
        "rule_id": RULE_ID,
        "stage_id": stage_id,
    })


def send_eof_report(
    channel,
    request_id: str,
    node_id: str,
    processed: int,
    emitted: int,
) -> None:
    publish(channel, COORDINATOR_QUEUE, {
        "event": "EOF_REPORT",
        "request_id": request_id,
        "node_id": node_id,
        "client_id": CLIENT_ID,
        "processed": processed,
        "emitted": emitted,
    })


def main() -> None:
    connection, channel = connect()
    heartbeat_stop_event = threading.Event()

    try:
        queues = [
            COORDINATOR_QUEUE,
            CURRENCY_NODE_1_QUEUE,
            CURRENCY_NODE_2_QUEUE,
            AMOUNT_NODE_1_QUEUE,
            AMOUNT_NODE_2_QUEUE,
        ]

        print("\n--- SETUP: Declare and purge queues ---")
        pause("Press ENTER to declare and purge queues...")

        for queue in queues:
            declare_queue(channel, queue)
            purge_queue(channel, queue)

        print("[SETUP OK] Queues declared and purged")

        print("\n--- STEP 1: HELLO nodes for both stages ---")
        pause("Press ENTER to send HELLO from all nodes...")

        send_hello(
            channel,
            node_id=CURRENCY_NODE_1_ID,
            rule_id=RULE_ID,
            stage_id=STAGE_1_ID,
            next_stage_id=STAGE_2_ID,
            control_queue=CURRENCY_NODE_1_QUEUE,
        )

        send_hello(
            channel,
            node_id=CURRENCY_NODE_2_ID,
            rule_id=RULE_ID,
            stage_id=STAGE_1_ID,
            next_stage_id=STAGE_2_ID,
            control_queue=CURRENCY_NODE_2_QUEUE,
        )

        send_hello(
            channel,
            node_id=AMOUNT_NODE_1_ID,
            rule_id=RULE_ID,
            stage_id=STAGE_2_ID,
            next_stage_id=None,
            control_queue=AMOUNT_NODE_1_QUEUE,
        )

        send_hello(
            channel,
            node_id=AMOUNT_NODE_2_ID,
            rule_id=RULE_ID,
            stage_id=STAGE_2_ID,
            next_stage_id=None,
            control_queue=AMOUNT_NODE_2_QUEUE,
        )

        pause("Press ENTER to read WELCOME messages from all nodes...")

        welcome_currency_1 = read_until_event(channel, CURRENCY_NODE_1_QUEUE, "WELCOME")
        welcome_currency_2 = read_until_event(channel, CURRENCY_NODE_2_QUEUE, "WELCOME")
        welcome_amount_1 = read_until_event(channel, AMOUNT_NODE_1_QUEUE, "WELCOME")
        welcome_amount_2 = read_until_event(channel, AMOUNT_NODE_2_QUEUE, "WELCOME")

        assert (welcome_currency_1.get("event") or welcome_currency_1.get("type")) == "WELCOME"
        assert (welcome_currency_2.get("event") or welcome_currency_2.get("type")) == "WELCOME"
        assert (welcome_amount_1.get("event") or welcome_amount_1.get("type")) == "WELCOME"
        assert (welcome_amount_2.get("event") or welcome_amount_2.get("type")) == "WELCOME"

        print("[ASSERT OK] All nodes received WELCOME")

        start_heartbeat_thread(
            [
                CURRENCY_NODE_1_ID,
                CURRENCY_NODE_2_ID,
                AMOUNT_NODE_1_ID,
                AMOUNT_NODE_2_ID,
            ],
            heartbeat_stop_event,
        )

        print("[INFO] Heartbeat thread started")

        print("\n--- STEP 2: Gateway sends CLIENT_INPUT_COMPLETED ---")
        pause("Press ENTER to send CLIENT_INPUT_COMPLETED...")

        publish(channel, COORDINATOR_QUEUE, {
            "event": "CLIENT_INPUT_COMPLETED",
            "client_id": CLIENT_ID,
            "expected_input": 10,
        })

        print("[INFO] Gateway input completed sent")

        print("\n================ STAGE 1: currency_filter ================")

        print("\n--- STEP 3: currency_filter detects EOF ---")
        pause("Press ENTER to send STAGE_EOF_DETECTED for currency_filter...")

        send_stage_eof_detected(channel, STAGE_1_ID)

        print("\n--- STEP 4: Coordinator sends EOF_REPORT_REQUEST to currency_filter nodes ---")
        pause("Press ENTER to read EOF_REPORT_REQUEST from currency_filter nodes...")

        currency_request_1 = read_until_event(channel, CURRENCY_NODE_1_QUEUE, "EOF_REPORT_REQUEST")
        currency_request_2 = read_until_event(channel, CURRENCY_NODE_2_QUEUE, "EOF_REPORT_REQUEST")

        assert currency_request_1["request_id"] == currency_request_2["request_id"]
        assert currency_request_1["client_id"] == CLIENT_ID
        assert currency_request_2["client_id"] == CLIENT_ID

        currency_request_id = currency_request_1["request_id"]

        print(f"[ASSERT OK] currency_filter request_id={currency_request_id}")

        print("\n--- STEP 5A: currency_filter_1 sends EOF_REPORT ---")
        pause("Press ENTER to send EOF_REPORT from currency_filter_1...")

        send_eof_report(
            channel,
            request_id=currency_request_id,
            node_id=CURRENCY_NODE_1_ID,
            processed=6,
            emitted=3,
        )

        print("[INFO] Coordinator should still be WAITING for currency_filter_2")

        print("\n--- STEP 5B: currency_filter_2 sends EOF_REPORT ---")
        pause("Press ENTER to send EOF_REPORT from currency_filter_2...")

        send_eof_report(
            channel,
            request_id=currency_request_id,
            node_id=CURRENCY_NODE_2_ID,
            processed=4,
            emitted=4,
        )

        print("[INFO] Coordinator should close currency_filter and create next stage with expected_input=7")

        print("\n--- STEP 6: Coordinator releases currency_filter nodes ---")
        pause("Press ENTER to read RELEASE_CLIENT from currency_filter nodes...")

        currency_release_1 = read_until_event(channel, CURRENCY_NODE_1_QUEUE, "RELEASE_CLIENT")
        currency_release_2 = read_until_event(channel, CURRENCY_NODE_2_QUEUE, "RELEASE_CLIENT")

        assert currency_release_1["client_id"] == CLIENT_ID
        assert currency_release_2["client_id"] == CLIENT_ID

        if "request_id" in currency_release_1:
            assert currency_release_1["request_id"] == currency_request_id

        if "request_id" in currency_release_2:
            assert currency_release_2["request_id"] == currency_request_id

        print("[ASSERT OK] currency_filter released")

        print("\n================ STAGE 2: amount_filter ================")

        print("\n--- STEP 7: amount_filter detects EOF ---")
        pause("Press ENTER to send STAGE_EOF_DETECTED for amount_filter...")

        send_stage_eof_detected(channel, STAGE_2_ID)

        print("\n--- STEP 8: Coordinator sends EOF_REPORT_REQUEST to amount_filter nodes ---")
        pause("Press ENTER to read EOF_REPORT_REQUEST from amount_filter nodes...")

        amount_request_1 = read_until_event(channel, AMOUNT_NODE_1_QUEUE, "EOF_REPORT_REQUEST")
        amount_request_2 = read_until_event(channel, AMOUNT_NODE_2_QUEUE, "EOF_REPORT_REQUEST")

        assert amount_request_1["request_id"] == amount_request_2["request_id"]
        assert amount_request_1["client_id"] == CLIENT_ID
        assert amount_request_2["client_id"] == CLIENT_ID

        amount_request_id = amount_request_1["request_id"]

        assert amount_request_id != currency_request_id

        print(f"[ASSERT OK] amount_filter request_id={amount_request_id}")
        print("[ASSERT OK] amount_filter request is different from currency_filter request")

        print("\n--- STEP 9A: amount_filter_1 sends EOF_REPORT ---")
        pause("Press ENTER to send EOF_REPORT from amount_filter_1...")

        send_eof_report(
            channel,
            request_id=amount_request_id,
            node_id=AMOUNT_NODE_1_ID,
            processed=3,
            emitted=1,
        )

        print("[INFO] Coordinator should still be WAITING for amount_filter_2")

        print("\n--- STEP 9B: amount_filter_2 sends EOF_REPORT ---")
        pause("Press ENTER to send EOF_REPORT from amount_filter_2...")

        send_eof_report(
            channel,
            request_id=amount_request_id,
            node_id=AMOUNT_NODE_2_ID,
            processed=4,
            emitted=2,
        )

        print("[INFO] Coordinator should close amount_filter")

        print("\n--- STEP 10: Coordinator releases amount_filter nodes ---")
        pause("Press ENTER to read RELEASE_CLIENT from amount_filter nodes...")

        amount_release_1 = read_until_event(channel, AMOUNT_NODE_1_QUEUE, "RELEASE_CLIENT")
        amount_release_2 = read_until_event(channel, AMOUNT_NODE_2_QUEUE, "RELEASE_CLIENT")

        assert amount_release_1["client_id"] == CLIENT_ID
        assert amount_release_2["client_id"] == CLIENT_ID

        if "request_id" in amount_release_1:
            assert amount_release_1["request_id"] == amount_request_id

        if "request_id" in amount_release_2:
            assert amount_release_2["request_id"] == amount_request_id

        print("[ASSERT OK] amount_filter released")

        print("\n[TEST OK] Multi-stage EOF coordination completed successfully")
        print(f"[TEST OK] stage_1_request_id={currency_request_id}")
        print("[TEST OK] stage_1 processed total = 10")
        print("[TEST OK] stage_1 emitted total = 7")
        print(f"[TEST OK] stage_2_request_id={amount_request_id}")
        print("[TEST OK] stage_2 processed total = 7")
        print("[TEST OK] stage_2 emitted total = 3")

    finally:
        heartbeat_stop_event.set()
        connection.close()


if __name__ == "__main__":
    main()