import csv
import os
import uuid
from batch import Batch

BATCH_SIZE_BYTES = int(os.getenv("BATCH_SIZE_BYTES", "1024"))

def build_batches(file_path: str, client_id: int):
    sequence_number = 0
    current_lines = []
    current_size = 0

    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for row in reader:
            line = ','.join(row) + '\n'
            line_size = len(line.encode('utf-8'))

            # Edge case: one line overcome BATCH_SIZE
            if line_size > BATCH_SIZE_BYTES and not current_lines:
                print(f"[WARN] line overcome BATCH_SIZE ({line_size} bytes), sending equal")
                yield Batch(
                    sequence_number=sequence_number,
                    lines=[line],
                    is_last=False
                )
                sequence_number += 1
                continue

            # If add this line, overcome BATCH_SIZE_BYTES. Add equal
            if current_size + line_size > BATCH_SIZE_BYTES and current_lines:
                yield Batch(
                    sequence_number=sequence_number,
                    lines=current_lines,
                    is_last=False
                )
                sequence_number += 1
                current_lines = []
                current_size = 0

            current_lines.append(line)
            current_size += line_size

        # Sending last batch
        if current_lines:
            yield Batch(
                sequence_number=sequence_number,
                lines=current_lines,
                is_last=True
            )
                
