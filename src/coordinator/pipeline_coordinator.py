import os
import json
import time
import uuid
import logging
import threading
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Any

from workers.consumers.queue_consumer import QueueConsumer
from workers.publishers.queue_publisher import QueuePublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

COORDINATOR_QUEUE = os.getenv("COORDINATOR_QUEUE", "coordinator_control_queue")

REPORT_RETRY_SECONDS = int(os.getenv("REPORT_RETRY_SECONDS", "5"))
NODE_TIMEOUT_SECONDS = int(os.getenv("NODE_TIMEOUT_SECONDS", "300"))
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "15"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "180"))


@dataclass
class NodeInfo:
    """Runtime metadata for a worker node registered in the coordinator."""

    # Unique identifier of the node instance.
    node_id: str

    # Logical stage handled by this node, used to group equivalent workers. (EX RULE ID)
    stage_id: str

    # Identifier of the next logical stage in the pipeline. Used by the coordinator to propagate the EOF once this stage is completed.
    next_stage_id: str | None

    # Rule
    rule_id: str

    # Queue used by the coordinator to send control messages to this node.
    control_queue: str

    # Current node status, for example ACTIVE, DOWN or STOPPED.
    status: str = "ACTIVE"

    # Last time the coordinator received activity from this node.
    last_seen: float = field(default_factory=time.time)


@dataclass
class EofRequest:
    """EOF coordination round for a specific client and rule."""

    # Unique identifier of this EOF coordination round.
    request_id: str

    # Client or batch being processed.
    client_id: str

    # Rule whose EOF completion is being validated.
    rule_id: str

    # Logical stage handled by this node, used to group equivalent workers.
    stage_id: str

    # Number of transactions this rule is expected to process.
    expected_input: int

    # Snapshot of node IDs expected to report for this EOF round.
    expected_nodes: set[str]

    # Latest report received from each node: processed and emitted counters.
    reports: dict[str, dict[str, int]] = field(default_factory=dict)

    # Current EOF round status, for example WAITING, COMPLETED or ERROR.
    status: str = "WAITING"

    # Number of times the coordinator requested reports for this round.
    retry_count: int = 0

    # Last time the coordinator requested reports for this round.
    last_retry_at: float = field(default_factory=time.time)


class PipelineCoordinator:
    
    def __init__(self) -> None:
        self.nodes: dict[str, NodeInfo] = {}

        # rule_id -> stage_id -> node_ids
        self.nodes_by_stage: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

        # stage_id -> next_stage_id
        self.next_stage_by_stage: defaultdict[str, dict[str, str]] = defaultdict(dict)

        # client_id -> stage_id -> expected_input
        self.expected_inputs: defaultdict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))

        # request_id -> EofRequest
        self.pending_requests: dict[str, EofRequest] = {}

        self.lock = threading.Lock()

        self.channel = QueueConsumer(COORDINATOR_QUEUE)
        self.publisher = QueuePublisher()

    def start(self) -> None:
        """Start the coordinator service.

        Starts the background retry and node monitoring loops, then begins consuming
        coordinator control messages from the configured queue.
        """

        logging.info("PipelineCoordinator started")
        logging.info("Coordinator queue: %s", COORDINATOR_QUEUE)

        threading.Thread(target=self.retry_loop, daemon=True).start()
        threading.Thread(target=self.node_monitor_loop, daemon=True).start()

        self.channel.start(self.on_message)

    def on_message(self, event: dict[str, Any]) -> None:
        """Process a coordinator control event.

        Delegates the event to the corresponding handler and logs any unexpected
        processing error without stopping the coordinator.
        """

        try:
            self.handle_event(event)

        except Exception:
            logging.exception("Error processing coordinator event")

    def handle_event(self, event: dict[str, Any]) -> None:
        """Route a control event to its specific handler.

        Uses the event type to dispatch the message to the corresponding coordinator
        handler. Unknown event types are logged and ignored.
        """

        event_type = event.get("event")

        if event_type == "HELLO":
            self.handle_hello(event)

        elif event_type == "HEARTBEAT":
            self.handle_heartbeat(event)

        elif event_type == "INITIAL_EOF":
            self.handle_initial_eof(event)

        elif event_type == "EOF_DETECTED":
            self.handle_eof_detected(event)

        elif event_type == "EOF_REPORT":
            self.handle_eof_report(event)

        elif event_type == "GOODBYE":
            self.handle_goodbye(event)

        else:
            logging.warning("Unknown event: %s | payload=%s", event_type, event)


    # ---------------------------------------------------------
    # Coordinator handlers
    # ---------------------------------------------------------

    def handle_hello(self, event: dict[str, Any]) -> None:
        """Register a worker node in the coordinator.

        Stores the node metadata, associates it with its rule and stage, optionally
        records the next stage in the pipeline, and sends back the heartbeat settings
        through the node control queue.
        """

        node_id = self.required(event, "node_id")
        stage_id = self.required(event, "stage_id")
        rule_id = self.required(event, "rule_id")
        control_queue = self.required(event, "control_queue")
        next_stage_id = event.get("next_stage_id")

        with self.lock:
            self.nodes[node_id] = NodeInfo(
                node_id=node_id,
                rule_id=rule_id,
                stage_id=stage_id,
                next_stage_id=next_stage_id,
                control_queue=control_queue,
                status="ACTIVE",
                last_seen=time.time(),
            )

            self.nodes_by_stage[rule_id][stage_id].add(node_id)

            if next_stage_id:
                self.next_stage_by_stage[rule_id][stage_id] = next_stage_id

        logging.info("Node registered | node_id=%s rule_id=%s next_rule_id=%s control_queue=%s", node_id, rule_id, next_stage_id, control_queue)

        self.publisher.publish(
            control_queue,
            {
                "event": "WELCOME",
                "node_id": node_id,
                "heartbeat_interval_seconds": HEARTBEAT_INTERVAL,
                "timeout_seconds": NODE_TIMEOUT_SECONDS,
            }
        )

    def handle_heartbeat(self, event: dict[str, Any]) -> None:
        """Update the liveness state of a registered node.

        Marks the node as active and refreshes its last seen timestamp. Heartbeats from
        unknown nodes are logged and ignored.
        """

        node_id = self.required(event, "node_id")

        with self.lock:
            node = self.nodes.get(node_id)

            if not node:
                logging.warning("Heartbeat from unknown node: %s", node_id)
                return

            node.status = "ACTIVE"
            node.last_seen = time.time()

    def handle_goodbye(self, event: dict[str, Any]) -> None:
        """Handle a graceful node shutdown notification.

        Marks the registered node as stopped so it is no longer considered active by
        the coordinator. Unknown nodes are ignored.
        """

        node_id = self.required(event, "node_id")

        with self.lock:
            node = self.nodes.get(node_id)

            if not node:
                return

            node.status = "STOPPED"

        logging.info("Node stopped | node_id=%s", node_id)

    def handle_initial_eof(self, event: dict[str, Any]) -> None:
        """Register the expected input for the first pipeline stage.

        Stores the total number of transactions sent by the gateway for a client. This
        value is later used to validate when the first stage has fully processed its
        input.
        """

        client_id = self.required(event, "client_id")
        to_stage_id = self.required(event, "to_stage_id")
        expected_input = int(self.required(event, "expected_input"))

        with self.lock:
            self.expected_inputs[client_id][to_stage_id] = expected_input

        logging.info("Initial expected input registered | client_id=%s rule_id=%s expected_input=%s", client_id, to_stage_id, expected_input)

    def handle_eof_detected(self, event: dict[str, Any]) -> None:
        """Open an EOF coordination round for a stage.

        Triggered when a node reports that it detected EOF for a client. The coordinator
        validates that the stage has a known expected input, takes a snapshot of the
        active nodes for that rule and stage, creates an EOF request, and asks those
        nodes to report their local counters.
        """
        client_id = self.required(event, "client_id")
        stage_id = self.required(event, "stage_id")
        rule_id = self.required(event, "rule_id")

        with self.lock:
            expected_input = self.expected_inputs[client_id].get(stage_id)

            if expected_input is None:
                logging.error("EOF detected but expected_input is unknown | client_id=%s stage_id=%s", client_id, stage_id)
                return

            active_nodes = {
                node_id
                for node_id in self.nodes_by_stage.get(rule_id, {}).get(stage_id, set())
                if self.nodes[node_id].status == "ACTIVE"
            }

            if not active_nodes:
                logging.error("EOF detected but no active nodes found | client_id=%s stage_id=%s", client_id, stage_id)
                return

            request_id = self.create_request_id(client_id, stage_id)

            request = EofRequest(
                request_id=request_id,
                client_id=client_id,
                stage_id=stage_id,
                rule_id=rule_id,
                expected_input=expected_input,
                expected_nodes=active_nodes,
            )

            self.pending_requests[request_id] = request

        logging.info("EOF round opened | request_id=%s client_id=%s rule_id=%s expected_input=%s expected_nodes=%s", request_id, client_id, stage_id, expected_input, sorted(active_nodes))
        self.request_reports(request_id)

    def handle_eof_report(self, event: dict[str, Any]) -> None:
        """Store a node EOF report and try to close the request.

        Updates the latest processed and emitted counters for the reporting node,
        ignoring unknown requests, completed requests, and reports from nodes that were
        not part of the EOF round snapshot. After storing the report, attempts to close
        the EOF request.
        """

        request_id = self.required(event, "request_id")
        node_id = self.required(event, "node_id")
        processed = int(self.required(event, "processed"))
        emitted = int(self.required(event, "emitted"))

        with self.lock:
            request = self.pending_requests.get(request_id)

            if not request:
                logging.warning("Report for unknown request_id=%s", request_id)
                return

            if request.status == "COMPLETED":
                logging.info("Ignoring report for completed request_id=%s", request_id)
                return

            if node_id not in request.expected_nodes:
                logging.warning(
                    "Report from unexpected node | request_id=%s node_id=%s",
                    request_id,
                    node_id,
                )
                return

            # Idempotencia: piso el reporte anterior del nodo, no sumo incremental.
            request.reports[node_id] = {
                "processed": processed,
                "emitted": emitted,
            }

        logging.info(
            "EOF_REPORT received | request_id=%s node_id=%s processed=%s emitted=%s",
            request_id,
            node_id,
            processed,
            emitted,
        )

        self.try_close_request(request_id)

    def try_close_request(self, request_id: str) -> None:
        """Try to complete an EOF coordination round.

        Checks whether all expected nodes have reported and whether the aggregated
        processed count matches the expected input. If the stage is complete, marks the
        request as completed, registers the emitted count as the next stage input,
        notifies the current stage to release client state, and starts the next EOF
        round.
        """
        with self.lock:
            request = self.pending_requests.get(request_id)

            if not request or request.status != "WAITING":
                return

            reported_nodes = set(request.reports.keys())
            missing_nodes = request.expected_nodes - reported_nodes

            total_processed = sum(
                report["processed"]
                for report in request.reports.values()
            )

            total_emitted = sum(
                report["emitted"]
                for report in request.reports.values()
            )

            logging.info(
                "Trying close | request_id=%s processed=%s/%s emitted=%s missing_nodes=%s",
                request_id,
                total_processed,
                request.expected_input,
                total_emitted,
                sorted(missing_nodes),
            )

            # Todavía faltan nodos por reportar.
            if missing_nodes:
                return

            # Reportaron todos, pero todavía no procesaron todo lo esperado.
            if total_processed < request.expected_input:
                return

            # Se procesó más de lo esperado: inconsistencia.
            if total_processed > request.expected_input:
                request.status = "ERROR"
                logging.error(
                    "Invalid EOF state: processed > expected_input | request_id=%s processed=%s expected=%s",
                    request_id,
                    total_processed,
                    request.expected_input,
                )
                return

            # Cierre correcto.
            next_stage_id = self.next_stage_by_stage[request.rule_id][request.stage_id]

            request.status = "COMPLETED"

            if not next_stage_id:
                logging.info(
                    "Pipeline finished for client_id=%s at rule_id=%s",
                    request.client_id,
                    request.rule_id,
                )
                return

            self.expected_inputs[request.client_id][next_stage_id] = total_emitted
        
        self.notify_current_stage_completed(request)

        logging.info(
            "Stage completed | request_id=%s client_id=%s rule_id=%s next_stage_id=%s next_expected_input=%s",
            request_id,
            request.client_id,
            request.rule_id,
            next_stage_id,
            total_emitted,
        )

        self.start_eof_round(
            client_id=request.client_id,
            rule_id=request.rule_id,
            stage_id=next_stage_id,
            expected_input=total_emitted,
        )

    def request_reports(self, request_id: str) -> None:
        """Request EOF reports from all nodes in a pending EOF round.

        Sends a report request to each node that belongs to the request snapshot and
        updates the retry metadata for the round.
        """
        with self.lock:
            request = self.pending_requests.get(request_id)

            if not request or request.status != "WAITING":
                return

            for node_id in request.expected_nodes:
                node = self.nodes[node_id]

                message = {
                    "event": "REQUEST_EOF_REPORT",
                    "request_id": request.request_id,
                    "client_id": request.client_id,
                    "rule_id": request.rule_id,
                }

                self.publisher.publish(node.control_queue, message)

            request.last_retry_at = time.time()
            request.retry_count += 1

        logging.info(
            "REQUEST_EOF_REPORT sent | request_id=%s retry_count=%s",
            request_id,
            request.retry_count,
        )

    def retry_loop(self) -> None:
        """Retry pending EOF report requests.

        Periodically scans waiting EOF rounds and re-sends report requests when the
        configured retry interval has elapsed.
        """
        while True:
            now = time.time()

            request_ids_to_retry: list[str] = []

            with self.lock:
                for request_id, request in self.pending_requests.items():
                    if request.status != "WAITING":
                        continue

                    if now - request.last_retry_at >= REPORT_RETRY_SECONDS:
                        request_ids_to_retry.append(request_id)

            for request_id in request_ids_to_retry:
                logging.info("Retrying report request | request_id=%s", request_id)
                self.request_reports(request_id)

            time.sleep(1)

    def node_monitor_loop(self) -> None:
        """Monitor registered node liveness.

        Periodically checks the last heartbeat timestamp of each active node and marks
        nodes as down when they exceed the configured timeout.
        """
        while True:
            now = time.time()

            with self.lock:
                for node_id, node in self.nodes.items():
                    if node.status in {"STOPPED", "DOWN"}:
                        continue

                    if now - node.last_seen > NODE_TIMEOUT_SECONDS:
                        node.status = "DOWN"

                        logging.warning(
                            "Node marked DOWN | node_id=%s rule_id=%s",
                            node.node_id,
                            node.rule_id,
                        )

            time.sleep(MONITOR_INTERVAL_SECONDS)

    def start_eof_round(self, client_id: str, rule_id: str, stage_id:str, expected_input: int) -> None:
        """Start an EOF coordination round for a stage.

        Creates a new EOF request using the currently active nodes for the given rule
        and stage, then asks those nodes to report their local counters. If no active
        nodes are available, the round is not started.
        """
        active_nodes = {
            node_id
            for node_id in self.nodes_by_stage[rule_id].get(stage_id, set())
            if self.nodes[node_id].status == "ACTIVE"
        }

        if not active_nodes:
            logging.error(
                "Cannot start EOF round: no active nodes found | client_id=%s rule_id=%s stage_id=%s expected_input=%s",
                client_id,
                rule_id,
                stage_id,
                expected_input,
            )

            # guardar estado de error por cliente/stage si es necesario.
            return

        request_id = self.create_request_id(client_id, rule_id)

        self.pending_requests[request_id] = EofRequest(
            request_id=request_id,
            client_id=client_id,
            rule_id=rule_id,
            stage_id=stage_id,
            expected_input=expected_input,
            expected_nodes=active_nodes,
        )

        self.request_reports(request_id)

    def notify_current_stage_completed(self, request: EofRequest) -> None:
        """Notify current-stage nodes that client state can be released.

        Sends a release message to every node that participated in the completed EOF
        round, allowing them to clean local counters, buffers, or temporary state for
        the client.
        """
        message = {
            "event": "RELEASE_CLIENT",
            "request_id": request.request_id,
            "client_id": request.client_id,
            "rule_id": request.rule_id,
            "stage_id": request.stage_id,
        }

        with self.lock:
            nodes_to_notify = list(request.expected_nodes)

            messages = []

            for node_id in nodes_to_notify:
                node = self.nodes.get(node_id)

                if not node:
                    continue

                messages.append((node.control_queue, message))

        for queue_name, payload in messages:
            self.publisher.publish(
                queue_name,
                payload,
            )

        logging.info(
            "RELEASE_CLIENT sent | request_id=%s client_id=%s rule_id=%s stage_id=%s nodes=%s",
            request.request_id,
            request.client_id,
            request.rule_id,
            request.stage_id,
            sorted(nodes_to_notify),
        )

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
    def create_request_id(client_id: str, rule_id: str) -> str:
        """Create a unique EOF request identifier.

        Builds an identifier using the client, rule, and a short random suffix.
        """
        return f"{client_id}:{rule_id}:{uuid.uuid4().hex[:8]}"


if __name__ == "__main__":
    PipelineCoordinator().start()