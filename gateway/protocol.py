"""帧级协议：JSON 文本帧解析、版本与字段约定。"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FrameType(str, Enum):
    TELEMETRY_FRAGMENT = "telemetry_fragment"
    TELEMETRY_ACK = "telemetry_ack"
    RESUME = "resume"
    COMMAND = "command"
    COMMAND_RESULT = "command_result"
    ERROR = "error"


class ProtocolVersion(str, Enum):
    V1 = "1"
    V2 = "2"


@dataclass(frozen=True)
class ParseError:
    """解析/校验失败的可区分原因。"""

    reason: str
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"reason": self.reason, "detail": self.detail}


# ---- 可区分的拒绝原因（全工程统一码表） ----
R_MALFORMED_JSON = "MALFORMED_JSON"
R_NOT_AN_OBJECT = "NOT_AN_OBJECT"
R_UNKNOWN_FRAME_TYPE = "UNKNOWN_FRAME_TYPE"
R_MISSING_FIELD = "MISSING_FIELD"
R_BAD_FIELD_TYPE = "BAD_FIELD_TYPE"
R_UNKNOWN_REQUIRED_FIELD = "UNKNOWN_REQUIRED_FIELD"
R_UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
R_BAD_FRAGMENT_INDEX = "BAD_FRAGMENT_INDEX"
R_DUPLICATE_FRAGMENT_CONFLICT = "DUPLICATE_FRAGMENT_CONFLICT"
R_FRAGMENT_HEADER_CONFLICT = "FRAGMENT_HEADER_CONFLICT"
R_FRAGMENT_GAP = "FRAGMENT_GAP"
R_DUPLICATE_SEQ_CONFLICT = "DUPLICATE_SEQ_CONFLICT"
R_DEVICE_UNKNOWN = "DEVICE_UNKNOWN"
R_DEVICE_ID_MISMATCH = "DEVICE_ID_MISMATCH"
R_COMMAND_UNAUTHORIZED = "COMMAND_UNAUTHORIZED"
R_COMMAND_UNSUPPORTED = "COMMAND_UNSUPPORTED"
R_COMMAND_HANDLER_ERROR = "COMMAND_HANDLER_ERROR"
R_COMMAND_QUEUE_FULL = "COMMAND_QUEUE_FULL"
R_COMMAND_DUPLICATE = "COMMAND_DUPLICATE"
# 凭 command_id 查询时：本网关从未见过该指令号（或已超出保留窗口被彻底清理）
R_COMMAND_NOT_FOUND = "COMMAND_NOT_FOUND"
# 指令在真正开始执行前被撤销：提交方与查询方都会得到这个稳定终态
R_COMMAND_CANCELED = "COMMAND_CANCELED"
# 撤销来得太晚：指令已开始执行或已到终态，不允许撤销（防止状态回退）
R_COMMAND_CANCEL_TOO_LATE = "COMMAND_CANCEL_TOO_LATE"
# 指令状态登记册已到容量上限且没有可清理的终态记录：新提交被明确拒绝
R_COMMAND_STATUS_LIMIT = "COMMAND_STATUS_LIMIT"


@dataclass
class Frame:
    type: FrameType
    raw: dict[str, Any] = field(default_factory=dict)


_INT_TYPES = (int,)


def _is_int(value: Any) -> bool:
    # bool 是 int 的子类，显式排除，避免 true 被当作 1
    return isinstance(value, int) and not isinstance(value, bool)


def parse_frame(data: str | bytes) -> Frame | ParseError:
    """解析一个完整的 JSON 文本帧。非法 JSON / 非对象 / 未知类型均被拒绝。"""
    try:
        text = data.decode("utf-8") if isinstance(data, bytes) else data
    except UnicodeDecodeError as exc:
        return ParseError(R_MALFORMED_JSON, f"invalid utf-8: {exc}")
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        return ParseError(R_MALFORMED_JSON, f"invalid json: {exc}")
    if not isinstance(obj, dict):
        return ParseError(R_NOT_AN_OBJECT, f"top-level json is {type(obj).__name__}")
    type_value = obj.get("type")
    if not isinstance(type_value, str):
        return ParseError(R_MISSING_FIELD, "field 'type' is required and must be string")
    try:
        frame_type = FrameType(type_value)
    except ValueError:
        return ParseError(R_UNKNOWN_FRAME_TYPE, f"type={type_value!r}")
    return Frame(type=frame_type, raw=obj)


@dataclass(frozen=True)
class FragmentHeader:
    device_id: str
    seq: int
    index: int
    total: int
    version: str
    payload_fragment: str


def parse_fragment_header(obj: dict[str, Any]) -> FragmentHeader | ParseError:
    """校验分片帧的头部字段。缺失与类型错误分别给出不同原因。"""
    str_fields = ("device_id", "version", "payload_fragment")
    int_fields = ("seq", "index", "total")
    values: dict[str, Any] = {}
    for name in str_fields:
        value = obj.get(name)
        if value is None:
            return ParseError(R_MISSING_FIELD, f"field {name!r} is required")
        if not isinstance(value, str):
            return ParseError(
                R_BAD_FIELD_TYPE, f"field {name!r} must be string, got {type(value).__name__}"
            )
        values[name] = value
    for name in int_fields:
        value = obj.get(name)
        if value is None:
            return ParseError(R_MISSING_FIELD, f"field {name!r} is required")
        if not _is_int(value):
            return ParseError(
                R_BAD_FIELD_TYPE, f"field {name!r} must be integer, got {type(value).__name__}"
            )
        values[name] = value
    if values["version"] not in (ProtocolVersion.V1.value, ProtocolVersion.V2.value):
        return ParseError(R_UNSUPPORTED_VERSION, f"version={values['version']!r}")
    seq, index, total = values["seq"], values["index"], values["total"]
    if seq < 0:
        return ParseError(R_BAD_FIELD_TYPE, "field 'seq' must be >= 0")
    if total <= 0:
        return ParseError(R_BAD_FRAGMENT_INDEX, "'total' must be >= 1")
    if not 0 <= index < total:
        return ParseError(R_BAD_FRAGMENT_INDEX, f"index {index} out of range [0,{total})")
    return FragmentHeader(
        device_id=values["device_id"],
        seq=seq,
        index=index,
        total=total,
        version=values["version"],
        payload_fragment=values["payload_fragment"],
    )
