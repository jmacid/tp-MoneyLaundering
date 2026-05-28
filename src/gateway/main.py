import os
import logging
import socket
import signal
import multiprocessing
import message_handler
from common import middleware, message_protocol
import uuid
import json
import csv 

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
EOF_CONTROL_QUEUE = os.environ["EOF_CONTROL_QUEUE"]

BANKS_CSV_PATH = os.getenv("BANKS_CSV_PATH", "banks.csv")
RESOLVERS_COUNT = int(os.getenv("RESOLVERS_COUNT", "1"))
RESOLVER_EXCHANGE = os.getenv("RESOLVER_EXCHANGE", "max_bank_transactions")


def load_bank_mapping(file_path):
    """Lee el archivo CSV y retorna un diccionario id_bank -> name_bank"""
    mapping = {}
    if not os.path.exists(file_path):
        logging.warning(f"File not founded in {file_path}. Continuing with empty bank mapping.")
        return mapping
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None) 
            for row in reader:
                if len(row) >= 2:
                    mapping[row[0].strip()] = row[1].strip()
    except Exception as e:
        logging.error(f"Error reading the banks CSV file: {e}")
    return mapping


def handle_client_request(client_socket, message_handler):
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    control_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, EOF_CONTROL_QUEUE)

    try:
        bank_map = load_bank_mapping(BANKS_CSV_PATH)
        if bank_map:
            logging.info(f"Sending catalog of {len(bank_map)} banks to the Resolver replicas")
            mapping_payload = {"bank_mapping": bank_map}
            serialized_map = message_protocol.internal.serialize(mapping_payload)
            
            for i in range(RESOLVERS_COUNT):
                resolver_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, RESOLVER_EXCHANGE, [f"{RESOLVER_EXCHANGE}{i}"]
                )
                resolver_exchange.send(serialized_map)
                resolver_exchange.close()
    except Exception as e:
        logging.error(f"Error distributing the banks catalog: {e}")

    transactions_sent = 0

    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)
            logging.info(f"message received: {message}")
            
            if message[0] == message_protocol.external.MsgType.TRANSACTION_RECORD:
                serialized_message = message_handler.serialize_data_message(message[1])
                output_queue.send(serialized_message)
                transactions_sent += 1
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            if message[0] == message_protocol.external.MsgType.END_OF_RECODS:
                logging.info(f"End of records: {message[1]}")
                eof_msg = json.dumps({
                    "client_id": message_handler.client_id,
                    "node": "gateway",
                    "emitted": transactions_sent
                })
                control_queue.send(eof_msg.encode('utf-8'))

                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                return
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    finally:
        output_queue.close()
        control_queue.close()


def handle_client_response(client_list):
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Listening to {INPUT_QUEUE} queue")
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)

            if isinstance(fields, list) and len(fields) == 1:
                target_client_id = fields[0]
                logging.info(f"Gateway received EOF for client {target_client_id[:8]}. Sending to client...")

                for idx, client_data in enumerate(client_list):
                    if client_data[0] == target_client_id:
                        target_socket = client_data[2]
                        message_protocol.external.send_msg(target_socket, message_protocol.external.MsgType.END_OF_RECODS)
                        client_list.pop(idx) 
                        break
                ack()
                return

            if not isinstance(fields, dict) or "client_id" not in fields:
                ack()
                return

            target_client_id = fields.pop("client_id")

            for client_data in client_list:
                if client_data[0] == target_client_id:
                    target_socket = client_data[2]
                    message_protocol.external.send_msg(
                        target_socket,
                        message_protocol.external.MsgType.MINOR_RESULT,
                        fields,
                    )
                    break

            ack()
        except Exception as e:
            logging.error(f"[_consume_result] Error: {e}")
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, __, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        client_id = str(uuid.uuid4())
                        logging.info(f"A new client has connected: {client_id[:8]}")
                        message_handler_instance = message_handler.MessageHandler(client_id)
                        client_list.append([client_id, message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
