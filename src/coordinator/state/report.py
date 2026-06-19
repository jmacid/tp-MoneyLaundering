from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Report:
    """Internal state of a node report for an EOF coordination round."""

    request_id: str
    node_id: str
    client_id: str
    processed: int
    emitted: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "node_id": self.node_id,
            "client_id": self.client_id,
            "processed": self.processed,
            "emitted": self.emitted,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Report":
        return Report(
            request_id=data["request_id"],
            node_id=data["node_id"],
            client_id=data["client_id"],
            processed=int(data["processed"]),
            emitted=int(data["emitted"]),
        )