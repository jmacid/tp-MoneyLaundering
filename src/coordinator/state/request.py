from dataclasses import dataclass, field
from typing import Any
import time


@dataclass
class Request:
    """Internal state of an EOF coordination round for one client, rule and stage."""

    request_id: str
    client_id: str
    rule_id: str
    stage_id: str
    expected_input: int
    expected_nodes: set[str] = field(default_factory=set)
    status: str = "WAITING"
    retry_count: int = 0
    last_retry_at: float = 0.0
    created_at: float = field(default_factory=time.time)

    def can_close(self) -> bool:
        return self.has_all_reports() and self.total_processed() == self.expected_input

    def mark_retry(self) -> None:
        self.retry_count += 1
        self.last_retry_at = time.time()

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "rule_id": self.rule_id,
            "stage_id": self.stage_id,
            "expected_input": self.expected_input,
            "expected_nodes": list(self.expected_nodes),
            "status": self.status,
            "retry_count": self.retry_count,
            "last_retry_at": self.last_retry_at,
            "created_at": self.created_at,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Request":
        return Request(
            request_id=data["request_id"],
            client_id=data["client_id"],
            rule_id=data["rule_id"],
            stage_id=data["stage_id"],
            expected_input=int(data["expected_input"]),
            expected_nodes=set(data.get("expected_nodes", [])),
            status=data.get("status", "WAITING"),
            retry_count=int(data.get("retry_count", 0)),
            last_retry_at=float(data.get("last_retry_at", 0.0)),
            created_at=float(data.get("created_at", time.time())),
        )