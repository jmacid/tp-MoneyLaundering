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

ROW_LIMIT = 1500 #None

class Client:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.output_file_minor_result = None
        self.csv_writer = None

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
        if self.output_file_minor_result:
            self.output_file_minor_result.close()
    
    def send_transaction_records(self, input_file):
        logging.info("[send_transaction_records] Sending transaction records")
        with open(input_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            _header = next(csv_reader)

            for row in islice(csv_reader, ROW_LIMIT):
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.TRANSACTION_RECORD,
                    row
                )

                while True:
                    msg_type, msg_payload = message_protocol.external.recv_msg(self.server_socket)
                    if msg_type == message_protocol.external.MsgType.ACK:
                        break
                    elif msg_type == message_protocol.external.MsgType.MINOR_RESULT:
                        self._save_minor_result(msg_payload)

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

    def receive_results(self):
        logging.info("[receive_results] Waiting for processed results....")
        while not self.closed:
            msg_type, msg_payload = message_protocol.external.recv_msg(self.server_socket)

            if msg_type == message_protocol.external.MsgType.MINOR_RESULT:
                self._save_minor_result(msg_payload)
            elif msg_type == message_protocol.external.MsgType.END_OF_RECODS:
                logging.info("All results received. Processing finished successfully.")
                break

    def _save_minor_result(self, msg_payload):
        logging.info(f"SUSPICIOUS MINOR TRANSACTION DETECTED: {msg_payload}")
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
