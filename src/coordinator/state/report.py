from dataclasses import dataclass, field
from typing import Any
import time


@dataclass
class Report:
    request_id: str
    node_id: str
    client_id: str
    processed: int
    emitted: int
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "node_id": self.node_id,
            "client_id": self.client_id,
            "processed": self.processed,
            "emitted": self.emitted,
            "created_at": self.created_at,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Report":
        return Report(
            request_id=data["request_id"],
            node_id=data["node_id"],
            client_id=data["client_id"],
            processed=int(data["processed"]),
            emitted=int(data["emitted"]),
            created_at=float(data.get("created_at", time.time())),
        )