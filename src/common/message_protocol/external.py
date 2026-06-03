from asyncio import IncompleteReadError
from . import external_serializer
import json

class MsgType:
    BATCH_RECORD = 1
    ACK = 2
    END_OF_RECODS = 3
    MINOR_RESULT = 4


def _recv_sized(socket, size):
    """
    Receives exactly 'size' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def _recv_string(socket):
    """Helper to receive a dynamically sized string"""
    str_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    return external_serializer.deserialize_string(_recv_sized(socket, str_size))


def _recv_transaction_record(socket):
    timestamp = _recv_string(socket)
    from_bank = _recv_string(socket)
    from_account = _recv_string(socket)
    to_bank = _recv_string(socket)
    to_account = _recv_string(socket)
    
    amount_received = external_serializer.deserialize_float(
        _recv_sized(socket, external_serializer.FLOAT_SIZE)
    )
    receiving_currency = _recv_string(socket)
    
    amount_paid = external_serializer.deserialize_float(
        _recv_sized(socket, external_serializer.FLOAT_SIZE)
    )
    payment_currency = _recv_string(socket)
    payment_format = _recv_string(socket)
    
    is_laundering = external_serializer.deserialize_bool(
        _recv_sized(socket, external_serializer.BOOL_SIZE)
    )
    return (
        timestamp, from_bank, from_account, to_bank, to_account, 
        amount_received, receiving_currency, amount_paid, 
        payment_currency, payment_format, is_laundering
    )

def _recv_minor_result(socket):
    return json.loads(_recv_string(socket))


def _recv_empty(socket):
    return None


RECV_MSG_HANDLERS = {
    MsgType.TRANSACTION_RECORD: _recv_transaction_record,
    # MsgType.TRANSACTION_TOP: _recv_transaction_top,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECODS: _recv_empty,
    MsgType.MINOR_RESULT: _recv_minor_result,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


def _serialize_string(s):
    """Helper to serialize a string with its size prefix"""
    return external_serializer.serialize_uint32(len(s)) + external_serializer.serialize_string(s)


def _serialize_transaction_record(timestamp, from_bank, from_account, to_bank, to_account, 
                                  amount_received, receiving_currency, amount_paid, 
                                  payment_currency, payment_format, is_laundering):
    return b"".join(
        [
            _serialize_string(timestamp),
            _serialize_string(from_bank),
            _serialize_string(from_account),
            _serialize_string(to_bank),
            _serialize_string(to_account),
            external_serializer.serialize_float(amount_received),
            _serialize_string(receiving_currency),
            external_serializer.serialize_float(amount_paid),
            _serialize_string(payment_currency),
            _serialize_string(payment_format),
            external_serializer.serialize_bool(is_laundering),
        ]
    )


def _send_transaction_record(socket, row):
    [timestamp, from_bank, from_account, to_bank, to_account, amount_received, receiving_currency, amount_paid, payment_currency, payment_format, is_laundering] = row
    msg = external_serializer.serialize_uint32(MsgType.TRANSACTION_RECORD)
    msg += _serialize_transaction_record(
        timestamp, from_bank, from_account, to_bank, to_account, 
        amount_received, receiving_currency, amount_paid, 
        payment_currency, payment_format, is_laundering
    )
    socket.sendall(msg)

def _send_minor_result(socket, result_dict):
    msg = external_serializer.serialize_uint32(MsgType.MINOR_RESULT)
    msg += _serialize_string(json.dumps(result_dict))
    socket.sendall(msg)

def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))


def _send_end_of_records(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECODS))


SEND_MSG_HANDLERS = {
    MsgType.TRANSACTION_RECORD: _send_transaction_record,
    # MsgType.TRANSACTION_TOP: _send_transaction_top,
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECODS: _send_end_of_records,
    MsgType.MINOR_RESULT: _send_minor_result,
}


def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)