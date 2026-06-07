import csv
import logging
import os
import uuid
from batch import Batch

BATCH_SIZE_BYTES = int(os.getenv("BATCH_SIZE_BYTES", "1024"))

def build_batches(file_path: str, client_id: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"[build_batches] file not found: {file_path}")
    
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
                logging.warning(f"[build_batches] line overcome BATCH_SIZE ({line_size} bytes), sending equal")
                logging.info(f"[build_batches] emitiendo batch {sequence_number}")
                yield Batch(
                    sequence_number=sequence_number,
                    lines=[line],
                    is_last=False,
                    client_id=client_id
                )
                sequence_number += 1
                continue

            # If add this line, overcome BATCH_SIZE_BYTES. Add equal
            if current_size + line_size > BATCH_SIZE_BYTES and current_lines:
                logging.info(f"[build_batches] emitiendo batch {sequence_number}")
                yield Batch(
                    sequence_number=sequence_number,
                    lines=current_lines,
                    is_last=False,
                    client_id=client_id
                )
                sequence_number += 1
                current_lines = []
                current_size = 0

            current_lines.append(line)
            current_size += line_size

        # Sending last batch
        if current_lines:
            logging.info(f"[build_batches] emitiendo batch {sequence_number}")
            yield Batch(
                sequence_number=sequence_number,
                lines=current_lines,
                is_last=True,
                client_id=client_id
            )
                
