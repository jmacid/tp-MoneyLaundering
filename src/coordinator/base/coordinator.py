from datetime import datetime
import os
import sys
import time
import uuid
import logging
import threading
from typing import Any
import time
import signal
from common.logging.color import blue, cyan, green, magenta, red, yellow
from coordinator.messages.inbound import (
    HelloMessage,
    HeartbeatMessage,
    GoodbyeMessage,
    ClientInputCompletedMessage,
    ReportMessage,
    StageEofDetectedMessage,
)
from coordinator.messages.outbound import WelcomeMessage, EofReportRequestMessage, ReleaseClientMessage
from coordinator.messages.parser import parse_inbound_message
from coordinator.state.client_input import ClientInput
from coordinator.state.report import Report
from coordinator.state.stage import Stage
from coordinator.storage.client_input_storage import ClientInputStorage
from coordinator.storage.node_storage import NodeStorage
from coordinator.state.request import Request
from coordinator.state.node import Node
from coordinator.storage.report_storage import ReportStorage
from coordinator.storage.request_storage import RequestStorage
from coordinator.storage.stage_storage import StageStorage
from workers.consumers.queue_consumer import QueueConsumer
from workers.publishers.queue_publisher import QueuePublisher

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=logging.WARNING, format="%(message)s | %(asctime)s", stream=sys.stdout)

COORDINATOR_ID = os.getenv("COORDINATOR_ID", "main")

logger = logging.getLogger(f"coordinator.{COORDINATOR_ID}")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.propagate = True

logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("pika.adapters").setLevel(logging.WARNING)
logging.getLogger("pika.adapters.utils").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

COORDINATOR_QUEUE = os.getenv("COORDINATOR_QUEUE", "coordinator_control_queue")

REPORT_RETRY_SECONDS = int(os.getenv("REPORT_RETRY_SECONDS", "5"))
NODE_TIMEOUT_SECONDS = int(os.getenv("NODE_TIMEOUT_SECONDS", "15"))
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "2"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "180"))


class Coordinator:

    def __init__(self) -> None:

        self.id = os.getenv("COORDINATOR_ID", "1")
        self.stop_event = threading.Event()

        self.channel = QueueConsumer(COORDINATOR_QUEUE)
        self.publisher = QueuePublisher()

        self.nodes = NodeStorage(os.getenv("STATE_DB_PATH", "/app/data/state.db"))
        self.stages = StageStorage(os.getenv("STATE_DB_PATH", "/app/data/state.db"))
        self.requests = RequestStorage(os.getenv("STATE_DB_PATH", "/app/data/state.db"))
        self.reports = ReportStorage(os.getenv("STATE_DB_PATH", "/app/data/state.db"))
        self.client_inputs = ClientInputStorage(os.getenv("STATE_DB_PATH", "/app/data/state.db"))
        self._pending_eof_detections: dict[str, list[StageEofDetectedMessage]] = {}

    def request_shutdown(self, signum, frame) -> None:
        logger.info("Shutdown signal received | signal=%s", signum)
        self.stop_event.set()

        try:
            if self.channel:
                self.channel.stop()
        except Exception:
            logging.exception("Error stopping coordinator consumer")

    def start(self) -> None:

        signal.signal(signal.SIGTERM, self.request_shutdown)
        signal.signal(signal.SIGINT, self.request_shutdown)

        try:
            logger.info("%s", magenta(f"Coordinator {self.id} is listening on queue: {COORDINATOR_QUEUE}"))

            threading.Thread(target=self.request_retry_loop, daemon=True).start()
            threading.Thread(target=self.node_monitor_loop, daemon=True).start()
            self.channel.start(self.on_message)

        except Exception:
            logging.exception("Coordinator %s crashed", self.id)
            raise
        finally:
            logger.info("%s", magenta(f"Coordinator {self.id} shutting down gracefully"))

            self.stop_event.set()

            try:
                if self.channel:
                    self.channel.stop()
            except Exception:
                logging.exception("Error stopping coordinator consumer")

            logger.info("Coordinator stopped")

    def node_monitor_loop(self) -> None:
        """Monitor active nodes and mark stale ones as DOWN."""

        while not self.stop_event.is_set():
            try:
                older_than = time.time() - NODE_TIMEOUT_SECONDS
                stale_nodes = self.nodes.find_stale_active_nodes(older_than)

                for node in stale_nodes.values():
                    self.nodes.update_status(node.node_id, "DOWN")

                    last_seen_str = datetime.fromtimestamp(node.last_seen).strftime("%Y-%m-%d %H:%M:%S")
                    logger.warning("%s node_id=%s rule_id=%s stage_id=%s last_seen=%s", yellow("[NODE_DOWN]"), node.node_id, node.rule_id, node.stage_id, last_seen_str)

            except Exception:
                logger.exception("[NODE_MONITOR_LOOP] unexpected error")

            self.stop_event.wait(MONITOR_INTERVAL_SECONDS)

    def on_message(self, event: dict[str, Any]) -> None:
        try:
            self.handle_event(event)
        except Exception:
            logging.exception("Error processing coordinator event")

    def handle_event(self, event: dict[str, Any]) -> None:
        message = parse_inbound_message(event)

        match message:
            case HelloMessage():
                self.handle_hello(message)

            case HeartbeatMessage():
                self.handle_heartbeat(message)

            case GoodbyeMessage():
                self.handle_goodbye(message)

            case ClientInputCompletedMessage():
                self.handle_client_input_completed(message)

            case StageEofDetectedMessage():
                self.handle_stage_eof_detected(message)

            case ReportMessage():
                self.handle_report(message)

            case _:
                logging.warning("Unknown message: %s", message)

    def handle_hello(self, message: HelloMessage) -> None:

        node = Node.from_hello(message)
        logger.info("%s received from node=%s rule=%s stage=%s", green("[HELLO]"), node.node_id, node.rule_id, node.stage_id)

        self.nodes.save(node)
        welcome = WelcomeMessage(heartbeat_interval=HEARTBEAT_INTERVAL, heartbeat_timeout=NODE_TIMEOUT_SECONDS)
        self.publisher.publish(node.control_queue, welcome.to_dict())
        logger.info("%s sent to node=%s", blue("[WELCOME]"), node.node_id)

    def handle_heartbeat(self, message: HeartbeatMessage) -> None:

        logger.info("%s received from node=%s", green("[HEARTBEAT]"), message.node_id)
        self.nodes.touch(message.node_id)

    def handle_goodbye(self, message: GoodbyeMessage) -> None:

        logger.info("%s received from node=%s", green("[GOODBYE]"), message.node_id)
        self.nodes.stop(message.node_id)

    def handle_client_input_completed(self, message: ClientInputCompletedMessage) -> None:

        logger.info("%s stage received for client_id=%s expected_input=%s", green("[CLIENT_INPUT_COMPLETED]"), message.client_id, message.expected_input)
        self.client_inputs.save(ClientInput(client_id=message.client_id, expected_input=message.expected_input))

        pending = self._pending_eof_detections.pop(message.client_id, [])
        for deferred in pending:
            logger.info("%s replaying deferred STAGE_EOF_DETECTED for client_id=%s rule_id=%s stage_id=%s", yellow("[DEFERRED_EOF_REPLAY]"), deferred.client_id, deferred.rule_id, deferred.stage_id)
            self.handle_stage_eof_detected(deferred)

    def handle_stage_eof_detected(self, message: StageEofDetectedMessage) -> None:
        """Create an EOF coordination request when a stage reaches EOF."""

        logger.info("%s received in rule_id=%s stage_id=%s", green("[STAGE_EOF_DETECTED]"), message.rule_id, message.stage_id)
        stage = self.stages.get(message.client_id, message.rule_id, message.stage_id)

        if stage is None:
            expected_input = self.client_inputs.get_expected_input(message.client_id)
        else:
            expected_input = stage.expected_input

        if expected_input is None:
            logger.info("%s client_id=%s rule_id=%s stage_id=%s reason=awaiting_client_input_completed", yellow("[EOF_DETECTED_DEFERRED]"), message.client_id, message.rule_id, message.stage_id)
            self._pending_eof_detections.setdefault(message.client_id, []).append(message)
            return

        stage = Stage(client_id=message.client_id, rule_id=message.rule_id, stage_id=message.stage_id, expected_input=expected_input)

        self.stages.save(stage)
        self.create_request_for_stage(stage)

    def create_request_for_stage(self, stage: Stage) -> None:
        """Create an EOF request for a stage and ask its active nodes to report."""

        active_nodes = self.nodes.find_by_stage(rule_id=stage.rule_id, stage_id=stage.stage_id, status="ACTIVE")

        if not active_nodes:
            logging.warning("%s client_id=%s rule_id=%s stage_id=%s reason=no_active_nodes", red("[EOF_REQUEST_NOT_CREATED]"), stage.client_id, stage.rule_id, stage.stage_id)
            return

        request = Request(
            request_id=self.create_request_id(stage.client_id, stage.rule_id, stage.stage_id),
            client_id=stage.client_id,
            rule_id=stage.rule_id,
            stage_id=stage.stage_id,
            expected_input=stage.expected_input,
            expected_nodes=set(active_nodes.keys()),
            last_retry_at=time.time(),
        )

        created_request = self.requests.create_if_absent(request)

        if created_request is None:
            logger.info("%s client_id=%s rule_id=%s stage_id=%s", red("[EOF_REQUEST_ALREADY_EXISTS]"), stage.client_id, stage.rule_id, stage.stage_id)
            return

        logger.info("%s request_id=%s client_id=%s rule_id=%s stage_id=%s expected_input=%s expected_nodes=%i", cyan("[EOF_REQUEST_CREATED]"), created_request.request_id, created_request.client_id, created_request.rule_id, created_request.stage_id, created_request.expected_input, len(created_request.expected_nodes))

        self.send_eof_report_request(created_request, active_nodes)

    def handle_report(self, message: ReportMessage) -> None:
        """Handle an EOF report sent by a worker node.

        Stores the report idempotently and tries to close the EOF request.
        """
        report = Report(request_id=message.request_id,
            client_id=message.client_id,
            node_id=message.node_id,
            processed=message.processed,
            emitted=message.emitted,
        )

        logger.info("%s request_id=%s client_id=%s " "node_id=%s processed=%s emitted=%s", green("[EOF_REPORT_RECEIVED]"), report.request_id, report.client_id, report.node_id, report.processed, report.emitted)

        self.reports.save(report)
        self.try_close_request(report.request_id)


    def release_client(self, request: Request) -> None:
        """Tell all expected active nodes that the client can be released."""

        active_nodes = self.nodes.find_by_stage(rule_id=request.rule_id, stage_id=request.stage_id, status="ACTIVE")

        message = ReleaseClientMessage(request_id=request.request_id, client_id=request.client_id)

        for node_id in sorted(request.expected_nodes):
            node = active_nodes.get(node_id)

            if node is None:
                logging.warning("[RELEASE_CLIENT_SKIPPED] request_id=%s node_id=%s reason=node_not_active", request.request_id, node_id)
                continue

            self.publisher.publish(node.control_queue, message.to_dict())
            logger.info("%s request_id=%s client_id=%s node_id=%s queue=%s", magenta("[RELEASE_CLIENT_SENT]"), request.request_id, request.client_id, node.node_id, node.control_queue,)

    def try_close_request(self, request_id: str) -> None:
        """Close an EOF request if all expected reports have been received."""

        logging.warning("%s request_id=%s reason=request_not_found", yellow("Trying to close request"), request_id)
        request = self.requests.get(request_id)

        if request is None:
            logging.warning("[EOF_CLOSE_IGNORED] request_id=%s reason=request_not_found", request_id)
            return

        if request.status != "WAITING":
            logging.info("[EOF_CLOSE_IGNORED] request_id=%s status=%s reason=request_not_waiting", request_id, request.status)
            return

        reports = self.reports.list_by_request(request_id)
        reported_nodes = set(reports.keys())

        missing_nodes = request.expected_nodes - reported_nodes

        if missing_nodes:
            logging.info("[EOF_CLOSE_PENDING] request_id=%s missing_nodes=%s", request_id, sorted(missing_nodes))
            return

        total_processed = sum(report.processed for report in reports.values())

        if total_processed != request.expected_input:
            logging.info(
                "[EOF_CLOSE_PENDING] request_id=%s expected_input=%s total_processed=%s",
                request_id,
                request.expected_input,
                total_processed,
            )
            return

        claimed_request = self.requests.claim_for_close(request_id)

        if claimed_request is None:
            logging.info("[EOF_CLOSE_ALREADY_CLAIMED] request_id=%s", request_id)
            return

        total_emitted = sum(report.emitted for report in reports.values())

        self.release_client(claimed_request)
        self.create_next_stage_if_needed(claimed_request, total_emitted)

        self.requests.mark_closed(request_id)

        logging.info(
            "[EOF_REQUEST_CLOSED] request_id=%s client_id=%s rule_id=%s stage_id=%s " "total_processed=%s total_emitted=%s",
            claimed_request.request_id,
            claimed_request.client_id,
            claimed_request.rule_id,
            claimed_request.stage_id,
            total_processed,
            total_emitted,
        )

    def create_next_stage_if_needed(self, request: Request, expected_input: int) -> None:
        """Create the next stage state using the total emitted by the closed request."""

        next_stage_id = self.nodes.get_next_stage_id(rule_id=request.rule_id, stage_id=request.stage_id)

        if next_stage_id is None:
            logger.info(magenta(f"[PIPELINE_STAGE_FINAL] client_id={request.client_id} rule_id={request.rule_id} stage_id={request.stage_id}"))
            return

        next_stage = Stage(client_id=request.client_id, rule_id=request.rule_id, stage_id=next_stage_id, expected_input=expected_input)
        self.stages.save(next_stage)
        logger.info("%s client_id=%s rule_id=%s stage_id=%s expected_input=%s", cyan("[NEXT_STAGE_CREATED]"), next_stage.client_id, next_stage.rule_id, next_stage.stage_id, next_stage.expected_input)

    def request_retry_loop(self) -> None:
        """Retry EOF report requests for WAITING requests."""

        while not self.stop_event.is_set():
            try:
                waiting_requests = self.requests.list_waiting()

                for request in waiting_requests.values():
                    self.retry_request_if_needed(request)

            except Exception:
                logging.exception("[REQUEST_RETRY_LOOP] unexpected error")

            self.stop_event.wait(MONITOR_INTERVAL_SECONDS)

    def retry_request_if_needed(self, request: Request) -> None:
        """Send EOF report request to nodes that have not reported yet."""

        now = time.time()

        if request.last_retry_at and now - request.last_retry_at < REPORT_RETRY_SECONDS:
            return

        reports = self.reports.list_by_request(request.request_id)
        reported_nodes = set(reports.keys())
        missing_nodes = request.expected_nodes - reported_nodes

        if not missing_nodes:
            self.try_close_request(request.request_id)
            return

        active_nodes = self.nodes.find_by_stage(rule_id=request.rule_id, stage_id=request.stage_id, status="ACTIVE")

        nodes_to_retry = {node_id: node for node_id, node in active_nodes.items() if node_id in missing_nodes}

        if not nodes_to_retry:
            logging.warning(
                "[EOF_REQUEST_RETRY_SKIPPED] request_id=%s reason=no_active_missing_nodes missing_nodes=%s",
                request.request_id,
                sorted(missing_nodes),
            )
            return

        self.send_eof_report_request(request, nodes_to_retry)

        request.retry_count += 1
        request.last_retry_at = now
        self.requests.save(request)

        logger.info(
            "[EOF_REQUEST_RETRIED] request_id=%s retry_count=%s missing_nodes=%s retried_nodes=%s",
            request.request_id,
            request.retry_count,
            sorted(missing_nodes),
            sorted(nodes_to_retry.keys()),
        )

    def send_eof_report_request(self, request: Request, nodes: dict[str, Node]) -> None:
        """Send an EOF report request to the given nodes."""

        message = EofReportRequestMessage(request_id=request.request_id, client_id=request.client_id)

        for node in nodes.values():
            self.publisher.publish(node.control_queue, message.to_dict())
            logger.info("%s request_id=%s client_id=%s node_id=%s queue=%s", blue("[EOF_REPORT_REQUEST_SENT]"), request.request_id, request.client_id, node.node_id, node.control_queue)

    @staticmethod
    def required(event: dict[str, Any], key: str) -> Any:
        """Return a required event field.

        Raises a ValueError when the field is missing or empty.
        """
        value = event.get(key)

        if value is None or value == "":
            raise ValueError(f"Missing required field: {key}")

        return value

    @staticmethod
    def create_request_id(client_id: str, rule_id: str, stage_id: str) -> str:
        """Create a unique EOF request identifier."""

        return f"{client_id}:{rule_id}:{stage_id}:{uuid.uuid4().hex[:8]}"


if __name__ == "__main__":
    Coordinator().start()
