from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import time

from coordinator.messages.inbound import HelloMessage


@dataclass
class Node:
    """Runtime metadata for a worker node registered in the coordinator."""

    node_id: str
    rule_id: str
    stage_id: str
    next_stage_id: str | None
    control_queue: str
    status: str = "ACTIVE"
    last_seen: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
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
    def from_dict(data: dict[str, Any]) -> "Node":
        return Node(
            node_id=data["node_id"],
            rule_id=data["rule_id"],
            stage_id=data["stage_id"],
            next_stage_id=data.get("next_stage_id"),
            control_queue=data["control_queue"],
            status=data.get("status", "ACTIVE"),
            last_seen=float(data.get("last_seen", time.time())),
        )

    @staticmethod
    def from_hello(message: HelloMessage) -> "Node":
        return Node(
            node_id=message.node_id,
            rule_id=message.rule_id,
            stage_id=message.stage_id,
            next_stage_id=message.next_stage_id,
            control_queue=message.control_queue,
        )