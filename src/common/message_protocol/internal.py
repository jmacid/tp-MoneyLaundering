import json

def serialize(message):
    return json.dumps(
        message, 
        default=lambda o: str(o) 
    ).encode("utf-8")

def deserialize(message):
    return json.loads(message.decode("utf-8"))