from typing import Any
from attr import dataclass

@dataclass
class TransactionBatch:
    sequence_number: int
    lines: list[dict[str, Any]]
    is_last: bool
    client_id: str

    def to_dict(self) -> dict:
            return {
                "sequence_number": self.sequence_number,
                "lines": self.lines,
                "is_last": self.is_last,
                "client_id": self.client_id
            }

#Se usa en el protocolo interno (entre nodos): dicts ya parseados