import argparse
import json
import os
import subprocess
import time
from typing import Any

import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
COORDINATOR_QUEUE = os.getenv("COORDINATOR_QUEUE", "coordinator_control_queue")
COORDINATOR_CONTAINER_NAME = os.getenv(
    "COORDINATOR_CONTAINER_NAME",
    "pipeline_coordinator",
)

QUEUE_ARGUMENTS = {"x-queue-type": "quorum"}

CLIENT_ID = "client_1"
RULE_ID = "q1"

STAGE_ID = "currency_filter"
NEXT_STAGE_ID = "amount_filter"

NODE_1 = "currency_filter_1"
NODE_2 = "currency_filter_2"

NODE_1_CONTROL_QUEUE = f"{NODE_1}_control_queue"
NODE_2_CONTROL_QUEUE = f"{NODE_2}_control_queue"

STEP_DELAY_SECONDS = float(os.getenv("STEP_DELAY_SECONDS", "2"))
RESTART_WAIT_SECONDS = int(os.getenv("RESTART_WAIT_SECONDS", "12"))


def log_title(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def log_step(step: str) -> None:
    print("\n" + "-" * 90)
    print(step)
    print("-" * 90)


def pause(seconds: float = STEP_DELAY_SECONDS) -> None:
    print(f"\n[WAIT] Sleeping {seconds} seconds...")
    time.sleep(seconds)


def connect() -> tuple[pika.BlockingConnection, pika.adapters.blocking_connection.BlockingChannel]:
    print(f"[INFO] Connecting to RabbitMQ | host={RABBITMQ_HOST}")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )

    channel = connection.channel()

    print("[OK] Connected to RabbitMQ")

    return connection, channel


def declare_queue(channel, queue_name: str) -> None:
    channel.queue_declare(
        queue=queue_name,
        durable=True,
        arguments=QUEUE_ARGUMENTS,
    )


def purge_queue(channel, queue_name: str) -> None:
    declare_queue(channel, queue_name)
    channel.queue_purge(queue=queue_name)

    print(f"[CLEANUP] Queue purged | queue={queue_name}")


def purge_test_queues(channel) -> None:
    log_step("Cleanup: purging coordinator and node control queues")

    for queue_name in [
        COORDINATOR_QUEUE,
        NODE_1_CONTROL_QUEUE,
        NODE_2_CONTROL_QUEUE,
    ]:
        purge_queue(channel, queue_name)


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
    timeout_seconds: int = 20,
) -> dict[str, Any] | None:
    print(
        f"\n[WAITING] event={expected_event} queue={queue_name} timeout={timeout_seconds}s"
    )

    started_at = time.time()

    while time.time() - started_at < timeout_seconds:
        message = get_message(channel, queue_name, timeout_seconds=1)

        if not message:
            continue

        if message.get("event") == expected_event:
            print(f"[OK] Expected event received | event={expected_event}")
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
    welcome_1 = wait_for_event(channel, NODE_1_CONTROL_QUEUE, "WELCOME")
    welcome_2 = wait_for_event(channel, NODE_2_CONTROL_QUEUE, "WELCOME")

    if not welcome_1:
        raise RuntimeError(f"No WELCOME received in {NODE_1_CONTROL_QUEUE}")

    if not welcome_2:
        raise RuntimeError(f"No WELCOME received in {NODE_2_CONTROL_QUEUE}")

    print("\n[OK] WELCOME received by both nodes")


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


def read_request_id(channel, timeout_seconds: int = 20) -> str:
    request_1 = wait_for_event(
        channel,
        NODE_1_CONTROL_QUEUE,
        "REQUEST_EOF_REPORT",
        timeout_seconds=timeout_seconds,
    )

    request_2 = wait_for_event(
        channel,
        NODE_2_CONTROL_QUEUE,
        "REQUEST_EOF_REPORT",
        timeout_seconds=timeout_seconds,
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

    print(f"\n[OK] Same request_id received by both nodes | request_id={request_id_1}")

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

    print(
        "\n[INFO] Partial report sent. Coordinator should persist this report "
        "but must NOT close the stage yet."
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

    print(
        "\n[INFO] Closing report sent. If persistence worked, coordinator should "
        "combine node 1 persisted report + node 2 new report."
    )


def kill_coordinator_container() -> None:
    log_step("Chaos: ksend restart manually to pipeline coordinator container")
    print("\n[WAIT] Waiting before killing coordinator so Docker restart policy is active...")
    time.sleep(15)

    # command = [
    #     "docker",
    #     "exec",
    #     COORDINATOR_CONTAINER_NAME,
    #     "sh",
    #     "-c",
    #     "kill -9 1",
    # ]

    # print(f"[CHAOS] Running: {' '.join(command)}")

    # result = subprocess.run(
    #     command,
    #     capture_output=True,
    #     text=True,
    #     check=False,
    # )

    # if result.stdout:
    #     print("[DOCKER STDOUT]")
    #     print(result.stdout.strip())

    # if result.stderr:
    #     print("[DOCKER STDERR]")
    #     print(result.stderr.strip())

    # if result.returncode != 0:
    #     raise RuntimeError(
    #         f"docker kill failed with exit code {result.returncode}"
    #     )

    print(
        f"\n[OK] Container killed. Docker should restart it automatically "
        f"because restart=unless-stopped is configured."
    )


def wait_for_coordinator_restart() -> None:
    log_step("Waiting for coordinator restart")

    for remaining in range(RESTART_WAIT_SECONDS, 0, -1):
        print(f"[WAIT] Coordinator restart wait... {remaining}s")
        time.sleep(1)

    print("\n[INFO] Restart wait finished. Coordinator should be running again.")


def show_coordinator_container_status() -> None:
    log_step("Docker status for coordinator")

    command = [
        "docker",
        "ps",
        "--filter",
        f"name={COORDINATOR_CONTAINER_NAME}",
        "--format",
        "table {{.Names}}\t{{.Status}}\t{{.Ports}}",
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    print(result.stdout.strip())

    if result.stderr:
        print(result.stderr.strip())


def read_retry_request_after_restart(
    channel,
    original_request_id: str,
) -> None:
    log_step("Reading REQUEST_EOF_REPORT after coordinator restart")

    print(
        "\n[INFO] The coordinator should reload the pending EOF request from SQLite "
        "and retry REQUEST_EOF_REPORT with the same request_id."
    )

    retry_request_1 = wait_for_event(
        channel,
        NODE_1_CONTROL_QUEUE,
        "REQUEST_EOF_REPORT",
        timeout_seconds=35,
    )

    retry_request_2 = wait_for_event(
        channel,
        NODE_2_CONTROL_QUEUE,
        "REQUEST_EOF_REPORT",
        timeout_seconds=35,
    )

    if not retry_request_1:
        raise RuntimeError(
            f"No retry REQUEST_EOF_REPORT received in {NODE_1_CONTROL_QUEUE}"
        )

    if not retry_request_2:
        raise RuntimeError(
            f"No retry REQUEST_EOF_REPORT received in {NODE_2_CONTROL_QUEUE}"
        )

    retry_request_id_1 = retry_request_1["request_id"]
    retry_request_id_2 = retry_request_2["request_id"]

    if retry_request_id_1 != original_request_id:
        raise RuntimeError(
            f"Node 1 retry request_id changed: "
            f"{retry_request_id_1} != {original_request_id}"
        )

    if retry_request_id_2 != original_request_id:
        raise RuntimeError(
            f"Node 2 retry request_id changed: "
            f"{retry_request_id_2} != {original_request_id}"
        )

    print(
        "\n[OK] Coordinator retried EOF report request after restart "
        f"with persisted request_id={original_request_id}"
    )


def read_release_client_messages(channel) -> None:
    release_1 = wait_for_event(
        channel,
        NODE_1_CONTROL_QUEUE,
        "RELEASE_CLIENT",
        timeout_seconds=25,
    )

    release_2 = wait_for_event(
        channel,
        NODE_2_CONTROL_QUEUE,
        "RELEASE_CLIENT",
        timeout_seconds=25,
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


def run_normal_flow(channel) -> None:
    log_title("TEST: Pipeline Coordinator EOF flow without coordinator crash")

    log_step("Step 1: Sending HELLO messages")
    send_hello_messages(channel)
    pause()

    log_step("Step 2: Reading WELCOME messages")
    read_welcome_messages(channel)
    pause()

    log_step("Step 3: Sending INITIAL_EOF")
    send_initial_eof(channel)
    pause()

    log_step("Step 4: Sending EOF_DETECTED")
    send_eof_detected(channel)
    pause()

    log_step("Step 5: Reading REQUEST_EOF_REPORT messages")
    request_id = read_request_id(channel)
    print(f"\n[INFO] request_id={request_id}")
    pause()

    log_step("Step 6: Sending partial EOF_REPORT from node 1")
    send_partial_report(channel, request_id)
    pause(4)

    log_step("Step 7: Sending closing EOF_REPORT from node 2")
    send_closing_report(channel, request_id)
    pause()

    log_step("Step 8: Reading RELEASE_CLIENT messages")
    read_release_client_messages(channel)

    log_title("TEST FINISHED OK")


def run_restart_flow(channel) -> None:
    log_title("TEST: Coordinator persistence and restart flow")

    log_step("Step 1: Sending HELLO messages")
    send_hello_messages(channel)
    pause()

    log_step("Step 2: Reading WELCOME messages")
    read_welcome_messages(channel)
    pause()

    log_step("Step 3: Sending INITIAL_EOF")
    send_initial_eof(channel)
    pause()

    log_step("Step 4: Sending EOF_DETECTED")
    send_eof_detected(channel)
    pause()

    log_step("Step 5: Reading initial REQUEST_EOF_REPORT messages")
    request_id = read_request_id(channel)
    print(f"\n[INFO] initial request_id={request_id}")
    pause()

    log_step("Step 6: Sending partial EOF_REPORT from node 1")
    send_partial_report(channel, request_id)

    print(
        "\n[CHECKPOINT] At this point the coordinator should have persisted:\n"
        f"  - EOF request_id={request_id}\n"
        f"  - report from node={NODE_1}\n"
        "  - status=WAITING\n"
        "\nNow we kill the coordinator before node 2 reports."
    )

    pause(4)

    kill_coordinator_container()
    wait_for_coordinator_restart()
    show_coordinator_container_status()

    print(
        "\n[INFO] The coordinator should reload its persisted state and retry "
        "REQUEST_EOF_REPORT for the same request_id."
    )

    read_retry_request_after_restart(channel, request_id)

    pause()

    log_step("Step 7: Sending closing EOF_REPORT from node 2 after restart")
    send_closing_report(channel, request_id)

    print(
        "\n[EXPECTED] Coordinator should close the stage using:\n"
        f"  - persisted node 1 report: processed=6 emitted=4\n"
        f"  - new node 2 report: processed=4 emitted=3\n"
        f"  - total_processed=10\n"
        f"  - total_emitted=7"
    )

    pause(3)

    log_step("Step 8: Reading RELEASE_CLIENT messages")
    read_release_client_messages(channel)

    log_title("RESTART TEST FINISHED OK")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual tester for PipelineCoordinator EOF and restart flow."
    )

    parser.add_argument(
        "--restart-test",
        action="store_true",
        help="Kill the coordinator after the first EOF_REPORT and verify persisted recovery.",
    )

    parser.add_argument(
        "--no-purge",
        action="store_true",
        help="Do not purge test queues before running.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    connection, channel = connect()

    try:
        if not args.no_purge:
            purge_test_queues(channel)
            pause(1)

        if args.restart_test:
            run_restart_flow(channel)
        else:
            run_normal_flow(channel)

    finally:
        print("\n[INFO] Closing RabbitMQ connection")
        connection.close()


if __name__ == "__main__":
    main()