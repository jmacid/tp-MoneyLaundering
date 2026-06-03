from dataclasses import dataclass
from batch import Batch

@dataclass
class PendingBatch:
    batch: Batch
    sent_at: float
    retries: int = 0