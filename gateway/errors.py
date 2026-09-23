"""网关内统一的错误码与异常。

错误码按"失败发生在哪一层、因为什么"进行细分，调用方与测试可依据
``RejectCode`` 区分拒绝原因，而不是只能看到一个笼统的失败。
"""

from __future__ import annotations

from enum import Enum


class RejectCode(str, Enum):
    """报文/指令被拒绝时的可区分原因。"""

    FRAME_NOT_JSON = "frame_not_json"
    FRAME_NOT_OBJECT = "frame_not_object"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    BAD_FIELD_TYPE = "bad_field_type"
    UNKNOWN_MESSAGE_TYPE = "unknown_message_type"
    UNSUPPORTED_VERSION = "unsupported_version"
    UNKNOWN_REQUIRED_FIELD = "unknown_required_field"
    FRAGMENT_INDEX_INVALID = "fragment_index_invalid"
    FRAGMENT_TOTAL_MISMATCH = "fragment_total_mismatch"
    FRAGMENT_CONFLICT = "fragment_conflict"
    PAYLOAD_NOT_BASE64 = "payload_not_base64"
    PAYLOAD_NOT_JSON = "payload_not_json"
    PAYLOAD_NOT_OBJECT = "payload_not_object"
    RESUME_FROM_MISMATCH = "resume_from_mismatch"
    SESSION_MISMATCH = "session_mismatch"
    UNAUTHORIZED_COMMAND = "unauthorized_command"
    COMMAND_QUEUE_FULL = "command_queue_full"
    COMMAND_DUPLICATE_REQUEST = "command_duplicate_request"
    DEVICE_UNKNOWN = "device_unknown"
    INTERNAL_ERROR = "internal_error"


class GatewayError(Exception):
    """所有可预期拒绝的基类。"""

    code: RejectCode = RejectCode.INTERNAL_ERROR

    def __init__(self, message: str, *, code: RejectCode | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code

    def to_result(self) -> dict:
        """转为统一的拒绝结果字典。"""
        return {"accepted": False, "code": self.code.value, "reason": str(self)}
