import json
import logging
import os
import signal
import socket
import multiprocessing
import uuid

from .message_handler.message_handler import MessageHandler
from common.message_protocol import external
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from domain.message_type import MessageType

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


def handle_client_request(client_socket, message_handler):
    output_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, OUTPUT_QUEUE)
    try:
        while True:
            message = external.recv_msg(client_socket)

            if message[0] == external.MsgType.TRANSACTION_RECORD:
                output_queue.send(message_handler.serialize_data_message(message[1]))
                external.send_msg(client_socket, external.MsgType.ACK)

            elif message[0] == external.MsgType.END_OF_RECODS:
                logging.info(f"End of records for client {message_handler.client_id[:8]}")
                output_queue.send(message_handler.serialize_eof_message(message[1]))
                external.send_msg(client_socket, external.MsgType.ACK)
                return

    except socket.error:
        logging.error("Connection with client lost during request handling")
    except Exception as e:
        logging.error(f"Unexpected error in handle_client_request: {e}")
    finally:
        output_queue.close()


def handle_client_response(client_list):
    input_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, INPUT_QUEUE)

    def _consume_result(body, ack, nack):
        client_index = -1
        try:
            message = json.loads(body.decode())
            msg_type = message.get("type")
            client_id = message.get("client_id")

            target_socket = None
            for idx, client_data in enumerate(client_list):
                if client_data[0] == client_id:
                    client_index = idx
                    target_socket = client_data[2]
                    break

            if not target_socket:
                logging.warning(f"Socket not found for client {client_id[:8]}")
                ack()
                return

            if msg_type == MessageType.TRANSACTION:
                # Resultado de una query — enviar al cliente
                external.send_msg(
                    target_socket,
                    external.MsgType.TRANSACTION_RECORD,
                    message,
                )
                external.recv_msg(target_socket)  # esperar ACK del cliente

            elif msg_type == MessageType.EOF:
                # Todas las queries terminaron para este cliente
                logging.info(f"All queries done for client {client_id[:8]}, closing connection")
                external.send_msg(target_socket, external.MsgType.END_OF_RECODS)
                client_list.pop(client_index)

            ack()

        except socket.error:
            logging.error(f"Connection lost with client during response")
            if client_index != -1:
                client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(f"Unexpected error in _consume_result: {e}")
            nack()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for client_data in client_list:
        client_data[2].shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)

        with multiprocessing.Pool(processes=os.cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                logging.info("Gateway listening for connections")

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
                        logging.info(f"New client connected: {client_id[:8]}")

                        handler = MessageHandler(client_id)
                        client_list.append([client_id, handler, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, handler),
                        )

                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("Socket error while waiting for connections")
                            return 1
                        return 0
                    except Exception as e:
                        logging.error(f"Unexpected error in main loop: {e}")
                        return 2
    return 0


if __name__ == "__main__":
    main()