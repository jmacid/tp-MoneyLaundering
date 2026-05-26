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
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

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
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)
    
    def send_transaction_records(self, input_file):
        logging.info("[send_transaction_records] Sending transaction records")
        with open(input_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            header = next(csv_reader)
            for row in islice(csv_reader, 5):
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.TRANSACTION_RECORD,
                    row
                )
                logging.info("[send_transaction_records]: recv i")
                msg_0, msg_1 = message_protocol.external.recv_msg(self.server_socket)
                logging.info(f"[send_transaction_records]: recv f: {msg_0} - {msg_1}")

        logging.info("[send_transaction_records]: END_OF_RECODS i")
        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECODS
        )
        message_protocol.external.recv_msg(self.server_socket)
        logging.info("[send_transaction_records]: END_OF_RECODS f")

def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()
    logging.info("Starting")

    try:
        pass
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
