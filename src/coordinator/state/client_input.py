from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ClientInput:
    client_id: str
    expected_input: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_id": self.client_id,
            "expected_input": self.expected_input,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ClientInput":
        return ClientInput(
            client_id=data["client_id"],
            expected_input=int(data["expected_input"]),
        )