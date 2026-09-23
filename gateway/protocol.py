"""报文协议类型定义。

连接帧（一行一个 JSON 对象）::

    {"v": 1, "type": "telemetry", "device": "...", "session": "...",
     "seq": 12, "frag": {"i": 0, "n": 1}, "payload_b64": "..."}

payload 由各分片按序拼接后 base64 解码、再按 JSON 解析得到。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .errors import RejectCode


SUPPORTED_VERSIONS = (1, 2)


class MessageType(str, Enum):
    TELEMETRY = "telemetry"
    ACK = "ack"


@dataclass(frozen=True)
class Fragment:
    """一条遥测报文的单个分片。"""

    index: int
    total: int
    payload_b64: str


@dataclass(frozen=True)
class Frame:
    """解析后的连接帧。"""

    version: int
    message_type: MessageType
    device_id: str
    session_id: str
    seq: int
    fragment: Fragment | None = None
    resume_from: int | None = None


@dataclass
class AcceptResult:
    """帧处理的统一结果。"""

    accepted: bool
    device_id: str | None = None
    session_id: str | None = None
    seq: int | None = None
    duplicate: bool = False
    completed: bool = False
    record: dict | None = None
    code: RejectCode | None = None
    reason: str | None = None

    def to_dict(self) -> dict:
        out: dict = {
            "accepted": self.accepted,
            "device_id": self.device_id,
            "session_id": self.session_id,
            "seq": self.seq,
            "duplicate": self.duplicate,
            "completed": self.completed,
        }
        if self.record is not None:
            out["record"] = self.record
        if self.code is not None:
            out["code"] = self.code.value
        if self.reason is not None:
            out["reason"] = self.reason
        return out


@dataclass(frozen=True)
class CommandRequest:
    """下行指令请求。"""

    device_id: str
    command: str
    request_id: str
    args: dict = field(default_factory=dict)


@dataclass
class CommandResult:
    """下行指令的提交/执行结果。"""

    accepted: bool
    request_id: str
    executed: bool = False
    state: dict | None = None
    duplicate: bool = False
    code: RejectCode | None = None
    reason: str | None = None

    def to_dict(self) -> dict:
        out: dict = {
            "accepted": self.accepted,
            "request_id": self.request_id,
            "executed": self.executed,
            "duplicate": self.duplicate,
        }
        if self.state is not None:
            out["state"] = self.state
        if self.code is not None:
            out["code"] = self.code.value
        if self.reason is not None:
            out["reason"] = self.reason
        return out
