import json
import logging
import os
from .batch import Batch

BATCH_SIZE_BYTES = int(os.getenv("BATCH_SIZE_BYTES", "1024"))

def build_batches(file_path: str, client_id: str, message_handler):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"[build_batches] file not found: {file_path}")
    
    sequence_number = 0
    current_lines = []
    current_size = 0

    with open(file_path, mode='r', encoding='utf-8') as f:
        header = f.readline() 

        for raw_line in f:
            if not raw_line.strip():
                continue
        
            transaction_dict = message_handler.serialize_data_message(raw_line)
            
            line_size = len(json.dumps(transaction_dict).encode('utf-8'))

            if line_size > BATCH_SIZE_BYTES and not current_lines:
                logging.warning(f"[build_batches] transaction overcomes BATCH_SIZE ({line_size} bytes), sending anyway")
                logging.info(f"[build_batches] emitting batch {sequence_number}")
                yield Batch(
                    sequence_number=sequence_number,
                    lines=[transaction_dict],
                    is_last=False,
                    client_id=client_id
                )
                sequence_number += 1
                continue

            # If add this line, overcome BATCH_SIZE_BYTES. Add equal
            if current_size + line_size > BATCH_SIZE_BYTES and current_lines:
                logging.info(f"[build_batches] emitting batch {sequence_number}")
                yield Batch(
                    sequence_number=sequence_number,
                    lines=current_lines,
                    is_last=False,
                    client_id=client_id
                )
                sequence_number += 1
                current_lines = []
                current_size = 0

            current_lines.append(transaction_dict)
            current_size += line_size

        # Sending last batch
        if current_lines:
            logging.info(f"[build_batches] emitting last batch {sequence_number}")
            yield Batch(
                sequence_number=sequence_number,
                lines=current_lines,
                is_last=True,
                client_id=client_id
            )
        else:
            logging.info(f"[build_batches] emitting empty last batch {sequence_number}")
            yield Batch(
                sequence_number=sequence_number,
                lines=[],
                is_last=True,
                client_id=client_id
            )