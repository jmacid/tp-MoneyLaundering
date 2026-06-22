import json
import os
import queue
import threading
import time
from collections import defaultdict
from typing import Any, Callable
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

class CoordinatorClient:
    """
    Reusable coordination client used by every worker node.

    Responsibilities:
    - Send HELLO on startup.
    - Receive WELCOME from coordinator.
    - Send periodic HEARTBEAT.
    - Keep processed/emitted counters by client_id.
    - Notify EOF_DETECTED when the worker receives EOF from its input queue.
    - Answer EOF_REQUEST with EOF_REPORT.
    - Handle RELEASE_CLIENT and ABORT_CLIENT from coordinator.
    - Send GOODBYE on shutdown.
    """

    def __init__(
        self,
        *,
        node_id: str | None = None,
        rule_id: str | None = None,
        stage_id: str | None = None,
        next_stage_id: str | None = None,
        control_queue: str | None = None,
        coordinator_queue: str | None = None,
        rabbitmq_host: str | None = None,
        heartbeat_interval: float | None = None,
        heartbeat_timeout: float | None = None,
        on_release_client: Callable[[str], None] | None = None,
        on_abort_client: Callable[[str], None] | None = None,
    ) -> None:
        self.node_id = node_id or os.environ["NODE_ID"]
        self.rule_id = rule_id or os.environ["RULE_ID"]
        self.stage_id = stage_id or os.environ["STAGE_ID"]
        self.next_stage_id = next_stage_id or os.getenv("NEXT_STAGE_ID")
        self._welcome_received = threading.Event()

        self.control_queue = (control_queue or os.getenv("CONTROL_QUEUE") or f"{self.node_id}_control_queue")
        self.coordinator_queue = (coordinator_queue or os.getenv("COORDINATOR_QUEUE") or "coordinator_control_queue")
        self.rabbitmq_host = rabbitmq_host or os.getenv("RABBITMQ_HOST", "localhost")

        self.heartbeat_interval = float(heartbeat_interval if heartbeat_interval is not None else os.getenv("HEARTBEAT_INTERVAL", "5"))

        self.heartbeat_timeout = float(heartbeat_timeout if heartbeat_timeout is not None else os.getenv("HEARTBEAT_TIMEOUT", "15"))

        self.on_release_client = on_release_client
        self.on_abort_client = on_abort_client

        self.processed_by_client: dict[str, int] = defaultdict(int)
        self.emitted_by_client: dict[str, int] = defaultdict(int)

        self._counters_lock = threading.Lock()
        self._stop_event = threading.Event()

        # Publisher thread owns its RabbitMQ connection/channel.
        # Other threads only enqueue messages.
        self._publish_queue: queue.Queue[dict[str, Any] | None] = queue.Queue()

        self._publisher_thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._control_thread: threading.Thread | None = None

        self._control_middleware: MessageMiddlewareQueueRabbitMQ | None = None
        self._publisher_middleware: MessageMiddlewareQueueRabbitMQ | None = None

        self._started = False

    # ---------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------

    def start(self) -> None:
        """
        Starts the coordination protocol:
        - publisher loop
        - control queue consumer
        - HELLO
        - heartbeat loop
        """

        if self._started:
            return

        self._started = True
        self._stop_event.clear()

        self._publisher_thread = threading.Thread(
            target=self._publisher_loop,
            name=f"{self.node_id}-coordinator-publisher",
            daemon=True,
        )
        self._publisher_thread.start()

        self._control_thread = threading.Thread(
            target=self._control_loop,
            name=f"{self.node_id}-control-listener",
            daemon=True,
        )
        self._control_thread.start()

        self.send_hello()

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.node_id}-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def stop(self) -> None:
        """
        Stops the coordination client gracefully.
        Sends GOODBYE and closes internal loops.
        """

        if not self._started:
            return

        self.send_goodbye()
        time.sleep(0.2)

        self._stop_event.set()

        try:
            if self._control_middleware:
                self._control_middleware.stop_consuming()
        except Exception:
            pass

        try:
            if self._control_middleware:
                self._control_middleware.close()
        except Exception:
            pass

        try:
            if self._publisher_middleware:
                self._publisher_middleware.close()
        except Exception:
            pass

        self._publish_queue.put(None)

        self._started = False
    # ---------------------------------------------------------------------
    # Public API used by workers
    # ---------------------------------------------------------------------

    def record_processed(self, client_id: str, amount: int = 1) -> None:
        with self._counters_lock:
            self.processed_by_client[client_id] += amount

    def record_emitted(self, client_id: str, amount: int = 1) -> None:
        with self._counters_lock:
            self.emitted_by_client[client_id] += amount

    def notify_eof_detected(self, client_id: str) -> None:
        """
        Called by a worker when it receives EOF from its normal input queue.
        The worker should not forward EOF by itself.
        The coordinator decides when the stage is globally complete.
        """

        self._enqueue_event(
            {
                "event": "STAGE_EOF_DETECTED",
                "client_id": client_id,
                "rule_id": self.rule_id,
                "stage_id": self.stage_id,
                "node_id": self.node_id,
            }
        )

    def notify_initial_eof(self, client_id: str, expected_input: int) -> None:
        """
        Useful for gateway nodes.

        The gateway is the first component that knows how many transactions
        entered a rule/stage for a given client.
        """

        self._enqueue_event(
            {
                "event": "CLIENT_INPUT_COMPLETED",
                "client_id": client_id,
                "rule_id": self.rule_id,
                "to_stage_id": self.stage_id,
                "expected_input": expected_input,
                "node_id": self.node_id,
            }
        )

    def get_counters(self, client_id: str) -> tuple[int, int]:
        with self._counters_lock:
            processed = self.processed_by_client.get(client_id, 0)
            emitted = self.emitted_by_client.get(client_id, 0)

        return processed, emitted

    # ---------------------------------------------------------------------
    # Messages to coordinator
    # ---------------------------------------------------------------------

    def send_hello(self) -> None:
        event: dict[str, Any] = {
            "event": "HELLO",
            "node_id": self.node_id,
            "rule_id": self.rule_id,
            "stage_id": self.stage_id,
            "control_queue": self.control_queue,
        }

        if self.next_stage_id:
            event["next_stage_id"] = self.next_stage_id

        self._enqueue_event(event)

    def send_heartbeat(self) -> None:
        self._enqueue_event(
            {
                "event": "HEARTBEAT",
                "node_id": self.node_id,
            }
        )

    def send_goodbye(self) -> None:
        self._enqueue_event(
            {
                "event": "GOODBYE",
                "node_id": self.node_id,
                "rule_id": self.rule_id,
                "stage_id": self.stage_id,
            }
        )

    def send_eof_report(self, request_id: str, client_id: str) -> None:
        processed, emitted = self.get_counters(client_id)

        self._enqueue_event(
            {
                "event": "EOF_REPORT",
                "request_id": request_id,
                "client_id": client_id,
                "rule_id": self.rule_id,
                "stage_id": self.stage_id,
                "node_id": self.node_id,
                "processed": processed,
                "emitted": emitted,
            }
        )

    # ---------------------------------------------------------------------
    # Control messages from coordinator
    # ---------------------------------------------------------------------

    def _handle_control_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("event")

        if event_type == "WELCOME":
            self._handle_welcome(event)

        elif event_type == "EOF_REPORT_REQUEST":
            self._handle_eof_request(event)

        elif event_type == "RELEASE_CLIENT":
            self._handle_release_client(event)

        elif event_type == "ABORT_CLIENT":
            self._handle_abort_client(event)

        else:
            print(f"[{self.node_id}] Unknown control event: {event}")

    def _handle_welcome(self, event: dict[str, Any]) -> None:
        heartbeat_interval = event.get("heartbeat_interval")
        timeout = event.get("timeout")

        if heartbeat_interval is not None:
            self.heartbeat_interval = float(heartbeat_interval)

        if timeout is not None:
            self.heartbeat_timeout = float(timeout)

        self._welcome_received.set()

        print(
            f"[{self.node_id}] Registered in coordinator. "
            f"heartbeat_interval={self.heartbeat_interval}, "
            f"timeout={self.heartbeat_timeout}"
        )
    
    def wait_until_registered(self, timeout: float | None = None) -> bool:
        return self._welcome_received.wait(timeout)

    def _handle_eof_request(self, event: dict[str, Any]) -> None:
        request_id = self._required(event, "request_id")
        client_id = self._required(event, "client_id")

        self.send_eof_report(
            request_id=request_id,
            client_id=client_id,
        )

    def _handle_release_client(self, event: dict[str, Any]) -> None:
        client_id = self._required(event, "client_id")

        with self._counters_lock:
            self.processed_by_client.pop(client_id, None)
            self.emitted_by_client.pop(client_id, None)

        if self.on_release_client:
            self.on_release_client(client_id)

        print(f"[{self.node_id}] Released client state: {client_id}")

    def _handle_abort_client(self, event: dict[str, Any]) -> None:
        client_id = self._required(event, "client_id")

        with self._counters_lock:
            self.processed_by_client.pop(client_id, None)
            self.emitted_by_client.pop(client_id, None)

        if self.on_abort_client:
            self.on_abort_client(client_id)

        print(f"[{self.node_id}] Aborted client state: {client_id}")

    # ---------------------------------------------------------------------
    # Threads
    # ---------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            self.send_heartbeat()
            self._stop_event.wait(self.heartbeat_interval)

    def _publisher_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._publisher_middleware = MessageMiddlewareQueueRabbitMQ(
                    host=self.rabbitmq_host,
                    queue_name=self.coordinator_queue,
                )

                while not self._stop_event.is_set():
                    event = self._publish_queue.get()

                    if event is None:
                        return

                    body = json.dumps(event)
                    self._publisher_middleware.send(body)

            except Exception as exc:
                print(f"[{self.node_id}] Publisher error: {exc}")
                time.sleep(2)

            finally:
                try:
                    if self._publisher_middleware:
                        self._publisher_middleware.close()
                except Exception:
                    pass

    def _control_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._control_middleware = MessageMiddlewareQueueRabbitMQ(
                    host=self.rabbitmq_host,
                    queue_name=self.control_queue,
                )

                print(f"[{self.node_id}] Listening control queue: {self.control_queue}")

                self._control_middleware.start_consuming(self._on_control_message)

            except Exception as exc:
                if not self._stop_event.is_set():
                    print(f"[{self.node_id}] Control listener error: {exc}")
                    time.sleep(2)

            finally:
                try:
                    if self._control_middleware:
                        self._control_middleware.close()
                except Exception:
                    pass

    def _on_control_message(self, body: bytes, ack, nack) -> None:
        try:
            event = json.loads(body.decode("utf-8"))
            self._handle_control_event(event)
            ack()

        except Exception as exc:
            print(f"[{self.node_id}] Error processing control message: {exc}")
            nack()

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

    def _enqueue_event(self, event: dict[str, Any]) -> None:
        if self._stop_event.is_set():
            return

        self._publish_queue.put(event)

    @staticmethod
    def _required(event: dict[str, Any], key: str) -> Any:
        value = event.get(key)

        if value is None:
            raise ValueError(f"Missing required field '{key}' in event: {event}")

        return value