from dataclasses import dataclass
from typing import Any

from coordinator.state.report import Report
from coordinator.state.stage import Stage

@dataclass(frozen=True)
class HelloMessage:
    """Message sent by a worker to register itself in the coordinator."""

    node_id: str
    rule_id: str
    stage_id: str
    control_queue: str
    next_stage_id: str | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "HelloMessage":
        return HelloMessage(
            node_id=data["node_id"],
            rule_id=data["rule_id"],
            stage_id=data["stage_id"],
            control_queue=data["control_queue"],
            next_stage_id=data.get("next_stage_id"),
        )

@dataclass(frozen=True)
class ClientInputCompletedMessage:
    """Message sent by the gateway when it finishes sending all transactions for a client."""

    client_id: str
    expected_input: int

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ClientInputCompletedMessage":
        return ClientInputCompletedMessage(
            client_id=data["client_id"],
            expected_input=int(data["expected_input"]),
        )

@dataclass(frozen=True)
class StageEofDetectedMessage:
    """Message sent by a node when it receives EOF for a client in a specific stage."""

    client_id: str
    rule_id: str
    stage_id: str

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "StageEofDetectedMessage":
        return StageEofDetectedMessage(
            client_id=data["client_id"],
            rule_id=data["rule_id"],
            stage_id=data["stage_id"],
        )

    def to_stage(self, expected_input: int) -> Stage:
        return Stage(
            client_id=self.client_id,
            rule_id=self.rule_id,
            stage_id=self.stage_id,
            expected_input=expected_input,
        )

@dataclass(frozen=True)
class ReportMessage:
    """Message sent by a node with its counters for an EOF report request."""

    request_id: str
    node_id: str
    client_id: str
    processed: int
    emitted: int

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ReportMessage":
        return ReportMessage(
            request_id=data["request_id"],
            node_id=data["node_id"],
            client_id=data["client_id"],
            processed=int(data["processed"]),
            emitted=int(data["emitted"]),
        )

    def to_report(self) -> Report:
        return Report(
            request_id=self.request_id,
            node_id=self.node_id,
            client_id=self.client_id,
            processed=self.processed,
            emitted=self.emitted,
        )

@dataclass(frozen=True)
class HeartbeatMessage:
    """Message sent by a node to indicate that it is still alive."""

    node_id: str

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "HeartbeatMessage":
        return HeartbeatMessage(
            node_id=data["node_id"],
        )

@dataclass(frozen=True)
class GoodbyeMessage:
    """Message sent by a node before shutting down gracefully."""

    node_id: str

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "GoodbyeMessage":
        return GoodbyeMessage(
            node_id=data["node_id"],
        )