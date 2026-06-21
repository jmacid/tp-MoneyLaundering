from typing import Any

from coordinator.messages.types import InboundEventType
from coordinator.messages.inbound import (
    HelloMessage,
    HeartbeatMessage,
    GoodbyeMessage,
    ClientInputCompletedMessage,
    StageEofDetectedMessage,
    ReportMessage,
)

def parse_inbound_message(data: dict[str, Any]):
    message_type = data.get("event")

    match message_type:
        case InboundEventType.HELLO:
            return HelloMessage.from_dict(data)

        case InboundEventType.HEARTBEAT:
            return HeartbeatMessage.from_dict(data)

        case InboundEventType.GOODBYE:
            return GoodbyeMessage.from_dict(data)

        case InboundEventType.CLIENT_INPUT_COMPLETED:
            return ClientInputCompletedMessage.from_dict(data)

        case InboundEventType.STAGE_EOF_DETECTED:
            return StageEofDetectedMessage.from_dict(data)

        case InboundEventType.EOF_REPORT:
            return ReportMessage.from_dict(data)

        case _:
            raise ValueError(f"Unknown inbound message type: {message_type}")