import os
import logging
import socket
import signal
import csv
import threading
import socket
import time
from common import message_protocol
from src.common.message_protocol.pending_batches_table import PendingBatchesTable
from src.common.message_protocol.batch_spliter import build_batches

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE_MINOR_RESULT = os.environ["OUTPUT_FILE_MINOR_RESULT"]

BATCH_SIZE_BYTES = int(os.getenv("BATCH_SIZE_BYTES", "1024"))
ACK_TIMEOUT_SECONDS = float(os.getenv("ACK_TIMEOUT_SECONDS", "5.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

EXPECTED_EOFS = int(os.getenv("EXPECTED_EOFS", "5"))
CLIENT_ID = os.getenv("CLIENT_ID")


class Client:
    def __init__(self):
        self.client_id = CLIENT_ID
        self.closed = False
        self._stop_event = threading.Event()
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.output_file_minor_result = None
        self.csv_writer = None

    def handle_sigterm(self, signum, frame):
        logging.info("[client] Recieved SIGTERM signal")
        self.closed = True
        if hasattr(self, '_stop_event'):
            self._stop_event.set() 
        self.disconnect()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.output_file_minor_result:
            self.output_file_minor_result.close()
            self.output_file_minor_result = None
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)
            self.server_socket = None
    
    def send_transaction_records(self, input_file):
        pending = PendingBatchesTable()
        eof_acked = threading.Event() 

        receiver_thread = threading.Thread(
            target=self._receiver_loop,
            args=(pending, self._stop_event, eof_acked)
        )
        receiver_thread.start()

        self._sender_loop(input_file, pending)

        while not pending.is_empty():
            self._retry_expired(pending)
            time.sleep(0.1)

        while not eof_acked.is_set():
            message_protocol.external.send_msg(
                self.server_socket,
                message_protocol.external.MsgType.END_OF_RECORDS
            )
            logging.info("[send_transaction_records] END_OF_RECORDS sended, waiting ACK")
            eof_acked.wait(timeout=ACK_TIMEOUT_SECONDS)

        logging.info("[send_transaction_records] EOF confirmed by the gateway")
        receiver_thread.join()

    def _sender_loop(self, input_file: str, pending: PendingBatchesTable):
        for batch in build_batches(input_file, self.client_id):

            while pending.is_full():
                self._retry_expired(pending)
                time.sleep(0.05)

            message_protocol.external.send_msg(
                self.server_socket,
                message_protocol.external.MsgType.BATCH_RECORD,
                batch
            )
            pending.add(batch)
            logging.info(f"[_sender_loop] Batch {batch.sequence_number} sent")

            self._retry_expired(pending)


    def _retry_expired(self, pending: PendingBatchesTable):
        for p in pending.get_expired():
            if p.retries >= MAX_RETRIES:
                logging.warning(
                    f"[_retry_expired] Batch {p.batch.sequence_number} "
                    f"Overcome MAX_RETRIES, removed"
                )
                pending.remove(p.batch.sequence_number)
                continue

            logging.info(
                f"[_retry_expired] Resending batch {p.batch.sequence_number}, "
                f"Try {p.retries + 1}/{MAX_RETRIES}"
            )
            message_protocol.external.send_msg(
                self.server_socket,
                message_protocol.external.MsgType.BATCH_RECORD,
                p.batch
            )
            pending.increment_retries(p.batch.sequence_number)


    def _receiver_loop(self, pending: PendingBatchesTable, stop_event: threading.Event, eof_acked: threading.Event):
        eofs_received = 0
        while not stop_event.is_set():
            try:
                self.server_socket.settimeout(0.1)
                msg_type, msg_payload = message_protocol.external.recv_msg(self.server_socket)

                if msg_type == message_protocol.external.MsgType.ACK:
                    sequence_number = msg_payload
                    pending.ack(sequence_number)
                    logging.info(f"[_receiver_loop] ACK received for batch {sequence_number}")

                elif msg_type == message_protocol.external.MsgType.ACK_EOF:
                    logging.info("[_receiver_loop] ACK_EOF received")
                    eof_acked.set()

                elif msg_type == message_protocol.external.MsgType.MINOR_RESULT:
                    self._save_minor_result(msg_payload)

                elif msg_type == message_protocol.external.MsgType.END_OF_RECORDS:
                    eofs_received += 1
                    logging.info(f"[_receiver_loop] EOF received ({eofs_received}/{EXPECTED_EOFS})")
                    if eofs_received >= EXPECTED_EOFS:
                        logging.info("[_receiver_loop] All EOFs received")
                        return

            except socket.timeout:
                continue

    def _save_minor_result(self, msg_payload):
        logging.info(f"result: {msg_payload}")
        file_exists = os.path.isfile(OUTPUT_FILE_MINOR_RESULT)

        if self.output_file_minor_result is None:
            self.output_file_minor_result = open(OUTPUT_FILE_MINOR_RESULT, "a")
            self.csv_writer = csv.writer(self.output_file_minor_result, delimiter=",", quotechar='"')
            if not file_exists:
                self.csv_writer.writerow(msg_payload.keys())
        self.csv_writer.writerow(msg_payload.values())

def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()
    logging.info("Starting")
    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_transaction_records(INPUT_FILE)
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
        return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()
    logging.info("Ending")
    return 0

if __name__ == "__main__":
    main()
