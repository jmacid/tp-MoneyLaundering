from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Stage:
    """Persisted EOF state for a client in a specific rule stage."""

    client_id: str
    rule_id: str
    stage_id: str
    expected_input: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_id": self.client_id,
            "rule_id": self.rule_id,
            "stage_id": self.stage_id,
            "expected_input": self.expected_input,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Stage":
        return Stage(
            client_id=data["client_id"],
            rule_id=data["rule_id"],
            stage_id=data["stage_id"],
            expected_input=int(data["expected_input"]),
        )