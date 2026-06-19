from dataclasses import dataclass
from typing import Any

from coordinator.messages.types import OutboundCommandType

@dataclass(frozen=True)
class EofReportRequestMessage:
    """Command sent by the coordinator to ask a node for its EOF counters."""

    request_id: str
    client_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "event": OutboundCommandType.EOF_REPORT_REQUEST,
            "request_id": self.request_id,
            "client_id": self.client_id,
        }

@dataclass(frozen=True)
class WelcomeMessage:
    """Command sent by the coordinator after successfully registering a node."""

    heartbeat_interval: int
    heartbeat_timeout: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "event": OutboundCommandType.WELCOME,
            "heartbeat_interval": self.heartbeat_interval,
            "heartbeat_timeout": self.heartbeat_timeout,
        }

@dataclass(frozen=True)
class ReleaseClientMessage:
    """Message sent by the coordinator when a client EOF round is safely closed."""

    request_id: str
    client_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "event": "RELEASE_CLIENT",
            "request_id": self.request_id,
            "client_id": self.client_id,
        }