import os
import logging
import socket
import signal
import csv
from common import message_protocol
from itertools import islice

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE_MINOR_RESULT = os.environ["OUTPUT_FILE_MINOR_RESULT"]
OUTPUT_FILE_MAX_PER_BANK = os.environ.get("OUTPUT_FILE_MAX_PER_BANK", "/output/max_per_bank.csv")
OUTPUT_FILE_LOWER_THAN_AVG = os.environ.get("OUTPUT_FILE_LOWER_THAN_AVG", "/output/lower_than_avg.csv")
OUTPUT_FILE_SCATTER_GATHER = os.environ.get("OUTPUT_FILE_SCATTER_GATHER", "/output/scatter_gather.csv")
OUTPUT_FILE_AMOUNT_ACCOUNTS = os.environ.get("OUTPUT_FILE_AMOUNT_ACCOUNTS", "/output/amount_accounts.csv")

ROW_LIMIT = 7500 #None
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))

class Client:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.output_file_minor_result = None
        self.csv_writer = None
        self.output_file_max_per_bank = None
        self.csv_writer_max_per_bank = None
        self.output_file_lower_than_avg = None
        self.csv_writer_lower_than_avg = None
        self.output_file_scatter_gather = None
        self.csv_writer_scatter_gather = None
        self.output_file_amount_accounts = None
        self.csv_writer_amount_accounts = None

    def handle_sigterm(self, signum, frame):
        logging.info("[client] Recieved SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        try:
            if self.server_socket:
                self.server_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        if self.output_file_minor_result:
            self.output_file_minor_result.close()
        if self.output_file_max_per_bank:
            self.output_file_max_per_bank.close()
        if self.output_file_lower_than_avg:
            self.output_file_lower_than_avg.close()
        if self.output_file_scatter_gather:
            self.output_file_scatter_gather.close()
        if self.output_file_amount_accounts:
            self.output_file_amount_accounts.close()
    
    def send_transaction_records(self, input_file):
        logging.info("[send_transaction_records] Sending transaction records")
        with open(input_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            _header = next(csv_reader)

            rows = list(islice(csv_reader, ROW_LIMIT))
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.TRANSACTION_BATCH,
                    batch
                )

                while True:
                    msg_type, msg_payload = message_protocol.external.recv_msg(self.server_socket)
                    if msg_type == message_protocol.external.MsgType.ACK:
                        break
                    elif msg_type == message_protocol.external.MsgType.MINOR_RESULT:
                        self._save_minor_result(msg_payload)
                    elif msg_type == message_protocol.external.MsgType.MAX_PER_BANK:
                        self._save_max_per_bank(msg_payload)
                    elif msg_type == message_protocol.external.MsgType.LOWER_THAN_AVG:
                        self._save_lower_than_avg(msg_payload)
                    elif msg_type == message_protocol.external.MsgType.SCATTER_GATHER_ACCOUNTS:
                        self._save_scatter_gather(msg_payload)
                    elif msg_type == message_protocol.external.MsgType.AMOUNT_ACCOUNTS:
                        self._save_amount_accounts(msg_payload)

        logging.info("[send_transaction_records]: Enviando END_OF_RECODS")
        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECODS
        )

        while True:
            msg_type, msg_payload = message_protocol.external.recv_msg(self.server_socket)
            if msg_type == message_protocol.external.MsgType.ACK:
                break
            elif msg_type == message_protocol.external.MsgType.MINOR_RESULT:
                self._save_minor_result(msg_payload)
            elif msg_type == message_protocol.external.MsgType.MAX_PER_BANK:
                self._save_max_per_bank(msg_payload)
            elif msg_type == message_protocol.external.MsgType.LOWER_THAN_AVG:
                self._save_lower_than_avg(msg_payload)
            elif msg_type == message_protocol.external.MsgType.SCATTER_GATHER_ACCOUNTS:
                self._save_scatter_gather(msg_payload)
            elif msg_type == message_protocol.external.MsgType.AMOUNT_ACCOUNTS:
                self._save_amount_accounts(msg_payload)

    def receive_results(self):
        logging.info("[receive_results] Waiting for processed results....")
        while not self.closed:
            msg_type, msg_payload = message_protocol.external.recv_msg(self.server_socket)

            if msg_type == message_protocol.external.MsgType.MINOR_RESULT:
                self._save_minor_result(msg_payload)
            elif msg_type == message_protocol.external.MsgType.MAX_PER_BANK:
                self._save_max_per_bank(msg_payload)
            elif msg_type == message_protocol.external.MsgType.LOWER_THAN_AVG:
                self._save_lower_than_avg(msg_payload)
            elif msg_type == message_protocol.external.MsgType.SCATTER_GATHER_ACCOUNTS:
                self._save_scatter_gather(msg_payload)
            elif msg_type == message_protocol.external.MsgType.AMOUNT_ACCOUNTS:
                self._save_amount_accounts(msg_payload)
            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                logging.info("All results received. Processing finished successfully.")
                break

    def _save_minor_result(self, msg_payload):
        logging.info(f"result: {msg_payload}")
        file_exists = os.path.isfile(OUTPUT_FILE_MINOR_RESULT)

        if self.output_file_minor_result is None:
            self.output_file_minor_result = open(OUTPUT_FILE_MINOR_RESULT, "a", buffering=1)
            self.csv_writer = csv.writer(self.output_file_minor_result, delimiter=",", quotechar='"')
            if not file_exists:
                self.csv_writer.writerow(msg_payload.keys())
        self.csv_writer.writerow(msg_payload.values())

    def _save_max_per_bank(self, msg_payload):
        logging.info(f"result: {msg_payload}")
        results = msg_payload.get("results", [])
        if not results:
            return

        if self.output_file_max_per_bank is None:
            file_exists = os.path.isfile(OUTPUT_FILE_MAX_PER_BANK)
            self.output_file_max_per_bank = open(OUTPUT_FILE_MAX_PER_BANK, "a", buffering=1)
            self.csv_writer_max_per_bank = csv.writer(self.output_file_max_per_bank, delimiter=",", quotechar='"')
            if not file_exists:
                self.csv_writer_max_per_bank.writerow(results[0].keys())

        for row in results:
            self.csv_writer_max_per_bank.writerow(row.values())

    def _save_lower_than_avg(self, msg_payload):
        logging.info(f"result: {msg_payload}")
        file_exists = os.path.isfile(OUTPUT_FILE_LOWER_THAN_AVG)

        if self.output_file_lower_than_avg is None:
            self.output_file_lower_than_avg = open(OUTPUT_FILE_LOWER_THAN_AVG, "a", buffering=1)
            self.csv_writer_lower_than_avg = csv.writer(self.output_file_lower_than_avg, delimiter=",", quotechar='"')
            if not file_exists:
                self.csv_writer_lower_than_avg.writerow(msg_payload.keys())
        self.csv_writer_lower_than_avg.writerow(msg_payload.values())

    def _save_scatter_gather(self, msg_payload):
        logging.info(f"result: {msg_payload}")
        paths = msg_payload.get("scatter_gather_paths", [])
        if not paths:
            return

        if self.output_file_scatter_gather is None:
            file_exists = os.path.isfile(OUTPUT_FILE_SCATTER_GATHER)
            self.output_file_scatter_gather = open(OUTPUT_FILE_SCATTER_GATHER, "a", buffering=1)
            self.csv_writer_scatter_gather = csv.writer(self.output_file_scatter_gather, delimiter=",", quotechar='"')
            if not file_exists:
                self.csv_writer_scatter_gather.writerow(["bridge_account", "origins", "destinations"])

        for path in paths:
            self.csv_writer_scatter_gather.writerow([
                path["bridge_account"],
                ";".join(path["origins"]),
                ";".join(path["destinations"]),
            ])

    def _save_amount_accounts(self, msg_payload):
        logging.info(f"result: {msg_payload}")
        counts = msg_payload.get("counts", {})
        if not counts:
            return

        if self.output_file_amount_accounts is None:
            file_exists = os.path.isfile(OUTPUT_FILE_AMOUNT_ACCOUNTS)
            self.output_file_amount_accounts = open(OUTPUT_FILE_AMOUNT_ACCOUNTS, "a", buffering=1)
            self.csv_writer_amount_accounts = csv.writer(self.output_file_amount_accounts, delimiter=",", quotechar='"')
            if not file_exists:
                self.csv_writer_amount_accounts.writerow(["payment_method", "count"])

        for payment_method, count in counts.items():
            self.csv_writer_amount_accounts.writerow([payment_method, count])


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()
    logging.info("Starting")

    try:
        pass
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_transaction_records(INPUT_FILE)
        client.receive_results()
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
