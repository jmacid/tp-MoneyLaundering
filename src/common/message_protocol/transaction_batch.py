from typing import Any
from attr import dataclass
import json

@dataclass
class TransactionBatch:
    sequence_number: int
    lines: list[Any]
    is_last: bool
    client_id: str

    def to_dict(self) -> dict:
        cleaned_lines = []
        for line in self.lines:
            if isinstance(line, bytes):
                cleaned_lines.append(json.loads(line.decode("utf-8")))
            elif isinstance(line, str):
                cleaned_lines.append(json.loads(line))
            else:
                cleaned_lines.append(line)

        return {
            "sequence_number": self.sequence_number,
            "lines": cleaned_lines,
            "is_last": self.is_last,
            "client_id": self.client_id
        }
#Se usa en el protocolo interno (entre nodos): dicts ya parseados