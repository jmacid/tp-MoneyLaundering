from dataclasses import dataclass

@dataclass
class Batch:
    sequence_number: int
    lines: list[str]
    is_last: bool
