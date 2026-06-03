import threading
import time
from batch import Batch
from pending_batch import PendingBatch
import os

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
ACK_TIMEOUT_SECONDS = float(os.getenv("ACK_TIMEOUT_SECONDS", "5.0"))
MAX_PENDING_BATCHES = int(os.getenv("MAX_PENDING_BATCHES", "10"))


class PendingBatchesTable:
    def __init__(self):
        self._pending: dict[int, PendingBatch] = {}
        self._lock = threading.Lock()

    def add(self, batch: Batch):
        with self._lock:
            self._pending[batch.sequence_number] = PendingBatch(
                batch=batch,
                sent_at=time.time()
            )

    def ack(self, sequence_number: int):
        with self._lock:
            self._pending.pop(sequence_number, None)

    def is_full(self) -> bool:
        with self._lock:
            return len(self._pending) >= MAX_PENDING_BATCHES

    def get_expired(self) -> list[PendingBatch]:
        now = time.time()
        with self._lock:
            return [
                p for p in self._pending.values()
                if now - p.sent_at > ACK_TIMEOUT_SECONDS
            ]

    def increment_retries(self, sequence_number: int):
        with self._lock:
            if sequence_number in self._pending:
                self._pending[sequence_number].retries += 1
                self._pending[sequence_number].sent_at = time.time()

    def remove(self, sequence_number: int):
        with self._lock:
            self._pending.pop(sequence_number, None)

    def is_empty(self) -> bool:
        with self._lock:
            return len(self._pending) == 0