import json
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


class Client:
    def __init__(self):
        self.server_socket = None
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("[client] Received SIGTERM signal")
        self.closed = True
        self.disconnect()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.server_socket.close()
            self.server_socket = None

    def send_transaction_records(self, input_file):
        logging.info("[client] Sending transaction records")
        with open(input_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader)  # skip header
            for row in islice(csv_reader, 5):
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.TRANSACTION_RECORD,
                    row,
                )
                msg_type, _ = message_protocol.external.recv_msg(self.server_socket)
                if msg_type != message_protocol.external.MsgType.ACK:
                    raise ValueError(f"Expected ACK, got {msg_type}")

        logging.info("[client] Sending END_OF_RECORDS")
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.END_OF_RECODS,
        )
        msg_type, _ = message_protocol.external.recv_msg(self.server_socket)
        if msg_type != message_protocol.external.MsgType.ACK:
            raise ValueError(f"Expected ACK after EOF, got {msg_type}")

    def receive_results(self):
        logging.info("[client] Waiting for query results")
        while True:
            msg_type, payload = message_protocol.external.recv_msg(self.server_socket)

            if msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                logging.info("[client] All results received, closing")
                return

            if msg_type == message_protocol.external.MsgType.QUERY_RESULT:
                logging.info(f"[client] Query result: {payload}")
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.ACK,
                )


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()
    logging.info("[client] Starting")
    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_transaction_records(INPUT_FILE)
        client.receive_results()
    except socket.error:
        if not client.closed:
            logging.error("[client] Connection with server was lost")
            return 1
    except Exception as e:
        logging.error(f"[client] Unexpected error: {e}")
        return 2
    finally:
        if not client.closed:
            client.disconnect()
    logging.info("[client] Ending")
    return 0


if __name__ == "__main__":
    main()