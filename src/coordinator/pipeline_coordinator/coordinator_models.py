from dataclasses import dataclass, field
from typing import Any
import time

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

    def to_dict(self) -> dict[str, Any]:
        """Serialize the node metadata into a JSON-compatible dictionary."""

        return {
            "node_id": self.node_id,
            "rule_id": self.rule_id,
            "stage_id": self.stage_id,
            "next_stage_id": self.next_stage_id,
            "control_queue": self.control_queue,
            "status": self.status,
            "last_seen": self.last_seen,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "NodeInfo":
        return NodeInfo(
            node_id=data["node_id"],
            rule_id=data["rule_id"],
            stage_id=data["stage_id"],
            next_stage_id=data.get("next_stage_id"),
            control_queue=data["control_queue"],
            status=data.get("status", "ACTIVE"),
            last_seen=float(data.get("last_seen", time.time())),
        )


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "rule_id": self.rule_id,
            "stage_id": self.stage_id,
            "expected_input": self.expected_input,
            "expected_nodes": sorted(self.expected_nodes),
            "reports": self.reports,
            "status": self.status,
            "retry_count": self.retry_count,
            "last_retry_at": self.last_retry_at,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "EofRequest":
        return EofRequest(
            request_id=data["request_id"],
            client_id=data["client_id"],
            rule_id=data["rule_id"],
            stage_id=data["stage_id"],
            expected_input=int(data["expected_input"]),
            expected_nodes=set(data.get("expected_nodes", [])),
            reports=data.get("reports", {}),
            status=data.get("status", "WAITING"),
            retry_count=int(data.get("retry_count", 0)),
            last_retry_at=float(data.get("last_retry_at", time.time())),
        )