from dataclasses import dataclass

@dataclass
class Batch:
    sequence_number: int
    lines: list[str]
    is_last: bool
    client_id: str
    
#El identificador único en el sistema es (client_id, sequence_number)
#Necesito file_id sí en el futuro un cliente puede mandar más de un archivo a la vez