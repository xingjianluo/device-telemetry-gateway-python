"""遥测报文的版本兼容校验与归一化。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gateway.protocol import (
    R_BAD_FIELD_TYPE,
    R_MISSING_FIELD,
    R_NOT_AN_OBJECT,
    R_UNKNOWN_REQUIRED_FIELD,
    R_UNSUPPORTED_VERSION,
    ParseError,
)


@dataclass(frozen=True)
class TelemetryRecord:
    device_id: str
    seq: int
    version: str
    temperature: float
    humidity: float | None
    location: str
    raw: dict[str, Any]


# 各版本必填字段（缺失 -> MISSING_FIELD）
REQUIRED_FIELDS: dict[str, frozenset[str]] = {
    "1": frozenset({"temperature", "location"}),
    "2": frozenset({"temperature", "humidity", "location"}),
}
# 各版本已知字段白名单
KNOWN_FIELDS: dict[str, frozenset[str]] = {
    "1": frozenset({"temperature", "location", "humidity"}),
    # v2 新增必填 humidity、新增可选 firmware
    "2": frozenset({"temperature", "location", "humidity", "firmware"}),
}
# 缺省值：同一版本下缺省语义固定、可复现；不同版本间允许不同
DEFAULTS: dict[str, dict[str, Any]] = {
    "1": {"humidity": None},
    "2": {"humidity": None, "firmware": "unknown"},  # humidity 的缺省不会被使用：v2 中它必填
}
# 字段类型（未列出的已知字段按 str 处理）
FIELD_TYPES: dict[str, tuple[type, ...]] = {
    "temperature": (int, float),
    "humidity": (int, float),
    "location": (str,),
    "firmware": (str,),
}
# 显式声明“设备自报必填”的保留字段名
REQUIRES_KEY = "requires"


def _type_ok(name: str, value: Any) -> bool:
    if value is None:
        return name in ("humidity",)  # 仅 humidity 允许显式 null
    types = FIELD_TYPES.get(name, (str,))
    if bool not in types and isinstance(value, bool):
        return False
    return isinstance(value, types)


def normalize_telemetry(
    device_id: str,
    seq: int,
    version: str,
    payload: dict[str, Any],
) -> TelemetryRecord | ParseError:
    """按协商版本校验并归一化一条重组完成的遥测报文。"""
    required = REQUIRED_FIELDS.get(version)
    known = KNOWN_FIELDS.get(version)
    if required is None or known is None:
        return ParseError(R_UNSUPPORTED_VERSION, f"version={version!r}")
    if not isinstance(payload, dict):
        return ParseError(R_NOT_AN_OBJECT, "reassembled payload must be a json object")

    # 1) 设备自报的必填字段：出现本版本未知字段必须明确拒绝，不按旧版本猜测解析
    declared = payload.get(REQUIRES_KEY, [])
    if not isinstance(declared, list) or any(not isinstance(n, str) for n in declared):
        return ParseError(R_BAD_FIELD_TYPE, f"{REQUIRES_KEY!r} must be a list of strings")
    for name in declared:
        if name not in known:
            return ParseError(
                R_UNKNOWN_REQUIRED_FIELD,
                f"field {name!r} declared required is unknown in version {version}",
            )

    # 2) 必填字段不可缺失（版本必填 ∪ 设备自报必填）；唯一允许显式 null 的是
    #    v1 中“可选且未被设备自报为必填”的 humidity
    _MISSING = object()
    all_required = required | frozenset(declared)
    for name in all_required:
        value = payload.get(name, _MISSING)
        if value is _MISSING:
            return ParseError(R_MISSING_FIELD, f"field {name!r} is required")
        if value is None:
            nullable_optional = (
                name == "humidity" and version == "1" and name not in declared
            )
            if not nullable_optional:
                return ParseError(R_MISSING_FIELD, f"field {name!r} must not be null")

    # 3) 已知字段类型校验；未知可选字段直接忽略（可选字段可忽略）
    for name, value in payload.items():
        if name == REQUIRES_KEY or name not in known:
            continue
        if not _type_ok(name, value):
            return ParseError(
                R_BAD_FIELD_TYPE, f"field {name!r} has invalid value {value!r}"
            )

    # 4) 应用该版本固定的缺省值（顺序无关，结果可复现）
    defaults = DEFAULTS[version]
    temperature = payload["temperature"]
    humidity = payload.get("humidity", defaults["humidity"])
    location = payload["location"]
    firmware = payload.get("firmware", defaults.get("firmware"))

    normalized_raw: dict[str, Any] = {
        "temperature": float(temperature),
        "humidity": (float(humidity) if humidity is not None else None),
        "location": location,
    }
    if "firmware" in known:
        normalized_raw["firmware"] = firmware

    return TelemetryRecord(
        device_id=device_id,
        seq=seq,
        version=version,
        temperature=float(temperature),
        humidity=(float(humidity) if humidity is not None else None),
        location=location,
        raw=normalized_raw,
    )
