"""帧解析与协议版本兼容规则。

规则要点：
- 仅接受网关已知的版本号；更高版本一律拒绝（``unsupported_version``），
  绝不按旧版本猜测解析。
- 必填字段缺失、类型错误分别给出不同拒绝原因
  （``missing_required_field`` / ``bad_field_type``）。
- 载荷按版本校验：必填字段不可缺失；未知的必填扩展（由载荷内
  ``__required_ext__`` 声明，模拟高版本新增的必填字段）必须明确拒绝
  （``unknown_required_field``）；未知可选字段一律忽略。
- 缺省语义由版本固定：v1 缺省 ``quality=0``、``tags=()``；v2 缺省
  ``quality=1``、``tags=()``。同一输入在同一版本下结果可复现。
"""

from __future__ import annotations

import json

from .errors import GatewayError, RejectCode
from .protocol import Fragment, Frame, MessageType

SUPPORTED_VERSIONS: tuple[int, ...] = (1, 2)

#: 各版本遥测载荷中的必填字段（稳定、可复现的缺省规则）。
REQUIRED_PAYLOAD_FIELDS: dict[int, tuple[str, ...]] = {
    1: ("ts", "metrics"),
    2: ("ts", "metrics"),
}

#: 各版本下可选字段的稳定缺省值。
DEFAULT_VALUES: dict[int, dict[str, object]] = {
    1: {"quality": 0, "tags": []},
    2: {"quality": 1, "tags": []},
}

#: 网关在各版本下实际认识的载荷字段。
KNOWN_PAYLOAD_FIELDS: dict[int, frozenset[str]] = {
    1: frozenset({"ts", "metrics", "quality", "tags"}),
    2: frozenset({"ts", "metrics", "quality", "tags", "location"}),
}


def _err(code: RejectCode, message: str) -> GatewayError:
    return GatewayError(message, code=code)


def _require_str(obj: dict, key: str) -> str:
    if key not in obj:
        raise _err(RejectCode.MISSING_REQUIRED_FIELD, f"缺少必填字段: {key}")
    value = obj[key]
    if not isinstance(value, str) or not value:
        raise _err(RejectCode.BAD_FIELD_TYPE, f"字段 {key} 必须是非空字符串")
    return value


def _require_int(obj: dict, key: str, *, minimum: int | None = None) -> int:
    if key not in obj:
        raise _err(RejectCode.MISSING_REQUIRED_FIELD, f"缺少必填字段: {key}")
    value = obj[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise _err(RejectCode.BAD_FIELD_TYPE, f"字段 {key} 必须是整数")
    if minimum is not None and value < minimum:
        raise _err(RejectCode.BAD_FIELD_TYPE, f"字段 {key} 必须 >= {minimum}")
    return value


def parse_frame(raw: str | bytes | dict) -> Frame:
    """把一段连接输入解析为 :class:`Frame`，非法输入抛 :class:`GatewayError`。"""
    if isinstance(raw, dict):
        obj = raw
    else:
        if isinstance(raw, bytes):
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise _err(RejectCode.FRAME_NOT_JSON, f"报文不是合法 UTF-8: {exc}") from exc
        elif isinstance(raw, str):
            text = raw
        else:
            raise _err(RejectCode.FRAME_NOT_JSON, f"不支持的输入类型: {type(raw)!r}")
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as exc:
            raise _err(RejectCode.FRAME_NOT_JSON, f"报文不是合法 JSON: {exc.msg}") from exc
    if not isinstance(obj, dict):
        raise _err(RejectCode.FRAME_NOT_OBJECT, "报文顶层必须是 JSON 对象")

    version = _require_int(obj, "v", minimum=1)
    if version not in SUPPORTED_VERSIONS:
        raise _err(
            RejectCode.UNSUPPORTED_VERSION,
            f"不支持的协议版本: v{version}，支持: {tuple(SUPPORTED_VERSIONS)}",
        )

    type_raw = _require_str(obj, "type")
    try:
        message_type = MessageType(type_raw)
    except ValueError as exc:
        raise _err(RejectCode.UNKNOWN_MESSAGE_TYPE, f"未知报文类型: {type_raw}") from exc

    device_id = _require_str(obj, "device")
    session_id = _require_str(obj, "session")
    seq = _require_int(obj, "seq", minimum=0)

    fragment: Fragment | None = None
    if "frag" in obj:
        if obj["frag"] is None:
            raise _err(RejectCode.BAD_FIELD_TYPE, "字段 frag 不能为 null")
        frag = obj["frag"]
        if not isinstance(frag, dict):
            raise _err(RejectCode.BAD_FIELD_TYPE, "字段 frag 必须是对象")
        index = _require_int(frag, "i", minimum=0)
        total = _require_int(frag, "n", minimum=1)
        if index >= total:
            raise _err(
                RejectCode.FRAGMENT_INDEX_INVALID,
                f"分片索引越界: i={index} >= n={total}",
            )
        payload_b64 = _require_str(obj, "payload_b64")
        fragment = Fragment(index=index, total=total, payload_b64=payload_b64)
    elif message_type is MessageType.TELEMETRY:
        # 遥测必须带分片（n=1 即单分片），防止残缺报文被误当成整包。
        raise _err(RejectCode.MISSING_REQUIRED_FIELD, "遥测报文缺少 frag/payload_b64")

    resume_from: int | None = None
    if "resume_from" in obj:
        resume_from = _require_int(obj, "resume_from", minimum=0)

    return Frame(
        version=version,
        message_type=message_type,
        device_id=device_id,
        session_id=session_id,
        seq=seq,
        fragment=fragment,
        resume_from=resume_from,
    )


def decode_payload(version: int, payload_bytes: bytes) -> dict:
    """对重组后的完整 payload 字节做 JSON 解码与版本校验。

    base64 解码已在分片入库时逐片完成，这里接收的是按序拼接后的原始字节。
    """
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _err(RejectCode.PAYLOAD_NOT_JSON, f"payload 解码后不是合法 JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise _err(RejectCode.PAYLOAD_NOT_OBJECT, "payload 顶层必须是 JSON 对象")

    for field_name in REQUIRED_PAYLOAD_FIELDS[version]:
        if field_name not in payload:
            raise _err(
                RejectCode.MISSING_REQUIRED_FIELD,
                f"v{version} payload 缺少必填字段: {field_name}",
            )
        if payload[field_name] is None:
            raise _err(
                RejectCode.BAD_FIELD_TYPE,
                f"v{version} payload 必填字段 {field_name} 不能为 null",
            )

    ts = payload["ts"]
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        raise _err(RejectCode.BAD_FIELD_TYPE, "payload.ts 必须是数值时间戳")
    metrics = payload["metrics"]
    if not isinstance(metrics, dict) or not metrics:
        raise _err(RejectCode.BAD_FIELD_TYPE, "payload.metrics 必须是非空对象")

    # 高版本设备可在 __required_ext__ 中声明"网关必须认识的新增必填字段"。
    # 网关不认识时必须明确拒绝，不得按旧版本猜测解析。
    required_ext = payload.get("__required_ext__", [])
    if not isinstance(required_ext, list):
        raise _err(RejectCode.BAD_FIELD_TYPE, "payload.__required_ext__ 必须是数组")
    known = KNOWN_PAYLOAD_FIELDS[version]
    for ext in required_ext:
        if not isinstance(ext, str) or ext not in known:
            raise _err(
                RejectCode.UNKNOWN_REQUIRED_FIELD,
                f"v{version} 存在网关无法识别的必填扩展字段: {ext!r}，拒绝按旧版本猜测解析",
            )
        if ext not in payload:
            raise _err(
                RejectCode.MISSING_REQUIRED_FIELD,
                f"v{version} 声明的必填扩展字段缺失: {ext}",
            )

    # 未知可选字段一律忽略；已知可选字段套用版本固定的缺省值。
    normalized = {
        "ts": ts,
        "metrics": metrics,
        "quality": payload.get("quality", DEFAULT_VALUES[version]["quality"]),
        "tags": payload.get("tags", list(DEFAULT_VALUES[version]["tags"])),
    }
    if "location" in known and "location" in payload:
        normalized["location"] = payload["location"]
    return normalized
