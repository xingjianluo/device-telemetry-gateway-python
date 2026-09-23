"""协议版本兼容：多版本共存、可选字段可忽略、必填不可缺失、
未知必填扩展必须拒绝、缺省语义按版本稳定可复现。
"""

from __future__ import annotations

from gateway.core import Gateway
from gateway.registry import DeviceRegistry

from .helpers import dump_case, telemetry_frame


def make_gateway() -> Gateway:
    reg = DeviceRegistry()
    reg.register("d1", set())
    reg.register("d2", set())
    return Gateway(reg)


def test_v1_and_v2_coexist_with_version_specific_defaults() -> None:
    """v1/v2 设备同时接入；缺省值按版本固定且可复现。"""
    gw = make_gateway()
    body = {"ts": 1, "metrics": {"t": 1}}

    f1 = telemetry_frame(1, body, device="d1", version=1)
    r1 = gw.handle_frame(f1)
    dump_case("v1 设备：缺省 quality", f1, r1)
    assert r1.accepted
    assert r1.record["payload"]["quality"] == 0  # v1 缺省 0
    assert r1.record["payload"]["tags"] == []

    f2 = telemetry_frame(1, body, device="d2", version=2)
    r2 = gw.handle_frame(f2)
    dump_case("v2 设备：缺省 quality", f2, r2)
    assert r2.record["payload"]["quality"] == 1  # v2 缺省 1

    # 可复现：同样输入再跑一次，缺省结果不变
    gw2 = make_gateway()
    rr1 = gw2.handle_frame(telemetry_frame(1, body, device="d1", version=1))
    rr2 = gw2.handle_frame(telemetry_frame(1, body, device="d2", version=2))
    assert rr1.record["payload"]["quality"] == 0
    assert rr2.record["payload"]["quality"] == 1


def test_optional_fields_ignored_when_unknown() -> None:
    """版本下未知的可选字段一律忽略，不报错。"""
    gw = make_gateway()
    body = {"ts": 1, "metrics": {"t": 1}, "future_optional_field": "x" * 10}
    f = telemetry_frame(1, body, version=1)
    r = gw.handle_frame(f)
    dump_case("v1 携带未知可选字段 future_optional_field", f, r)
    assert r.accepted
    assert "future_optional_field" not in r.record["payload"]


def test_v2_location_supported_v1_ignores_unknown_optional() -> None:
    gw = make_gateway()
    body = {"ts": 1, "metrics": {"t": 1}, "location": [31.1, 121.2]}
    r2 = gw.handle_frame(telemetry_frame(1, body, device="d2", version=2))
    assert r2.record["payload"]["location"] == [31.1, 121.2]
    r1 = gw.handle_frame(telemetry_frame(1, body, device="d1", version=1))
    # v1 不认识 location，作为未知可选字段忽略
    assert "location" not in r1.record["payload"]


def test_required_field_missing_rejected_not_guessed() -> None:
    """必填字段缺失：按对应版本明确拒绝，不猜测。"""
    gw = make_gateway()
    for version, device in ((1, "d1"), (2, "d2")):
        f = telemetry_frame(1, {"ts": 1}, device=device, session=f"s{version}", version=version)
        r = gw.handle_frame(f)
        dump_case(f"v{version} 缺必填 metrics", f, r)
        assert not r.accepted and r.code.value == "missing_required_field"


def test_unknown_required_extension_must_be_rejected() -> None:
    """高版本声明网关不认识的必填扩展：明确拒绝，绝不按旧版本解析。"""
    gw = make_gateway()
    body = {
        "ts": 1,
        "metrics": {"t": 1},
        "firmware_hash": "abcdef",
        "__required_ext__": ["firmware_hash"],
    }
    f = telemetry_frame(1, body, version=1)
    r = gw.handle_frame(f)
    dump_case("v1 携带未知必填扩展 firmware_hash", f, r)
    assert not r.accepted and r.code.value == "unknown_required_field"
    assert gw.state_snapshot("d1") is None or gw.state_snapshot("d1")["record_count"] == 0


def test_unsupported_newer_version_rejected() -> None:
    """更高版本号整体拒绝，拒绝原因与缺字段可区分。"""
    gw = make_gateway()
    f = telemetry_frame(1, {"ts": 1, "metrics": {"t": 1}}, version=9)
    r = gw.handle_frame(f)
    dump_case("v9 未知版本", f, r)
    assert not r.accepted and r.code.value == "unsupported_version"
