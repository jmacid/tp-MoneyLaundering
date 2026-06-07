from asyncio import IncompleteReadError
from . import external_serializer
from batch import Batch
import json

class MsgType:
    BATCH_RECORD = 1
    BANK_MAPPING = 2
    ACK = 3
    END_OF_RECORDS = 4
    MINOR_RESULT = 5

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

def _recv_minor_result(socket):
    return json.loads(_recv_string(socket))

def _recv_empty(socket):
    return None

def _serialize_string(s):
    """Helper to serialize a string with its size prefix"""
    return external_serializer.serialize_uint32(len(s)) + external_serializer.serialize_string(s)

def _recv_batch(socket):
    client_id = _recv_string(socket)
    sequence_number = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    is_last = external_serializer.deserialize_bool(
        _recv_sized(socket, external_serializer.BOOL_SIZE)
    )
    lines_count = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    lines = [_recv_string(socket) for _ in range(lines_count)]
    return Batch(
        client_id=client_id,
        sequence_number=sequence_number,
        is_last=is_last,
        lines=lines
    )


def _recv_ack(socket):
    sequence_number = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    return sequence_number


RECV_MSG_HANDLERS = {
    MsgType.BATCH_RECORD: _recv_batch,
    MsgType.BANK_MAPPING: _recv_batch,
    MsgType.ACK: _recv_ack,
    MsgType.END_OF_RECORDS: _recv_empty,
    MsgType.MINOR_RESULT: _recv_minor_result,
}

def _send_batch(socket, msg_type, batch):
    msg = external_serializer.serialize_uint32(msg_type)
    msg += _serialize_string(batch.client_id)
    msg += external_serializer.serialize_uint32(batch.sequence_number)
    msg += external_serializer.serialize_bool(batch.is_last)
    msg += external_serializer.serialize_uint32(len(batch.lines))
    for line in batch.lines:
        msg += _serialize_string(line)
    socket.sendall(msg)

# They don't need msg_type
def _send_ack(socket, msg_type, sequence_number):
    msg = external_serializer.serialize_uint32(MsgType.ACK)
    msg += external_serializer.serialize_uint32(sequence_number)
    socket.sendall(msg)

def _send_end_of_records(socket, msg_type):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECORDS))

def _send_minor_result(socket, msg_type, result_dict):
    msg = external_serializer.serialize_uint32(MsgType.MINOR_RESULT)
    msg += _serialize_string(json.dumps(result_dict))
    socket.sendall(msg)


SEND_MSG_HANDLERS = {
    MsgType.BATCH_RECORD: _send_batch,
    MsgType.BANK_MAPPING: _send_batch,
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECORDS: _send_end_of_records,
    MsgType.MINOR_RESULT: _send_minor_result,
}

def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, msg_type, *args)