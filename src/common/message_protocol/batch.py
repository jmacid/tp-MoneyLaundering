from dataclasses import dataclass
import json
from typing import Any

@dataclass
class Batch:
    sequence_number: int
    lines: list[Any]
    is_last: bool
    client_id: str

    def to_dict(self) -> dict:
        return {
            "sequence_number": self.sequence_number,
            "lines": self.lines,
            "is_last": self.is_last,
            "client_id": self.client_id
        }
    
#El identificador único en el sistema es (client_id, sequence_number)
#Necesito file_id sí en el futuro un cliente puede mandar más de un archivo a la vez

#Se usa en el protocolo externo (cliente → gateway): strings crudos del CSV