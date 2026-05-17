import struct

UINT32_SIZE = 4
BOOL_SIZE = 1
FLOAT_SIZE = 8

def serialize_bool(u):
    return int(u).to_bytes(BOOL_SIZE, "big")

def deserialize_bool(b):
    return int.from_bytes(b, byteorder="big", signed=False)

def serialize_uint32(u):
    return int(u).to_bytes(UINT32_SIZE, "big", signed=False)

def deserialize_uint32(b):
    return int.from_bytes(b, byteorder="big", signed=False)

def deserialize_string(b):
    return b.decode("utf-8")

def serialize_string(s):
    return s.encode("utf-8")

def serialize_float(f):
    return struct.pack("!d", float(f))

def deserialize_float(b):
    return struct.unpack("!d", b)[0]