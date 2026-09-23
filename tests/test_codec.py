"""分片重组 / 乱序 / 重复分片 / 残缺与非法报文测试。

每个用例都打印：输入报文、判定依据（accepted/code/reason）、最终状态。
"""

from __future__ import annotations

import base64
import json

import pytest

from gateway.assembler import Reassembler
from gateway.codec import decode_payload
from gateway.core import Gateway
from gateway.errors import GatewayError
from gateway.protocol import Fragment
from gateway.registry import DeviceRegistry

from .helpers import dump_case, telemetry_frame


def make_gateway() -> Gateway:
    reg = DeviceRegistry()
    reg.register("d1", set())
    return Gateway(reg)


def test_fragment_reassembly_out_of_order() -> None:
    """分片乱序到达也能按索引重组，且跨非 3 字节边界的 base64 正确。"""
    gw = make_gateway()
    payload = json.dumps({"ts": 100, "metrics": {"temp": 36.6, "hum": 40}}).encode()
    assert len(payload) % 3 != 0  # 刻意构造非对齐边界
    chunks = [payload[:7], payload[7:20], payload[20:]]

    r1 = gw.handle_frame(telemetry_frame(1, i=2, n=3, raw_payload=chunks[2]))
    dump_case("乱序分片 i=2 先到", telemetry_frame(1, i=2, n=3, raw_payload=chunks[2]), r1)
    assert r1.accepted and not r1.completed

    r2 = gw.handle_frame(telemetry_frame(1, i=0, n=3, raw_payload=chunks[0]))
    assert r2.accepted and not r2.completed

    r3 = gw.handle_frame(telemetry_frame(1, i=1, n=3, raw_payload=chunks[1]))
    snap = gw.state_snapshot("d1")
    dump_case("最后一片到达，重组完成", telemetry_frame(1, i=1, n=3, raw_payload=chunks[1]), r3, snap)
    assert r3.completed and r3.record is not None
    assert r3.record["payload"]["metrics"] == {"temp": 36.6, "hum": 40}
    assert snap["record_count"] == 1
    assert gw.reassembler.pending_count() == 0  # 完成后缓存被清理


def test_identical_fragment_retransmit_is_idempotent() -> None:
    """未集齐阶段，完全相同的分片重传幂等，不报冲突。"""
    r = Reassembler()
    ok, _ = r.add("s1", 1, Fragment(0, 2, base64.b64encode(b"abc").decode()))
    assert not ok
    ok, _ = r.add("s1", 1, Fragment(0, 2, base64.b64encode(b"abc").decode()))
    assert not ok
    ok, assembled = r.add("s1", 1, Fragment(1, 2, base64.b64encode(b"def").decode()))
    assert ok and assembled == b"abcdef"


def test_conflicting_fragment_rejected_with_distinct_reason() -> None:
    """同索引分片内容不同 -> fragment_conflict，与其他错误可区分。"""
    r = Reassembler()
    r.add("s1", 1, Fragment(0, 2, base64.b64encode(b"abc").decode()))
    with pytest.raises(GatewayError) as ei:
        r.add("s1", 1, Fragment(0, 2, base64.b64encode(b"XYZ").decode()))
    assert ei.value.code.value == "fragment_conflict"


def test_fragment_total_mismatch_rejected() -> None:
    r = Reassembler()
    r.add("s1", 1, Fragment(0, 2, base64.b64encode(b"a").decode()))
    with pytest.raises(GatewayError) as ei:
        r.add("s1", 1, Fragment(1, 3, base64.b64encode(b"b").decode()))
    assert ei.value.code.value == "fragment_total_mismatch"


def test_fragment_index_out_of_range_rejected() -> None:
    gw = make_gateway()
    f = telemetry_frame(1, i=2, n=2, raw_payload=b"x")
    r = gw.handle_frame(f)
    dump_case("分片索引越界 i>=n", f, r)
    assert not r.accepted and r.code.value == "fragment_index_invalid"


def test_invalid_base64_fragment_rejected() -> None:
    gw = make_gateway()
    f = telemetry_frame(1, raw_payload=b"x")
    f["payload_b64"] = "@@@not-base64@@@"
    r = gw.handle_frame(f)
    dump_case("分片 payload 非法 base64", f, r)
    assert not r.accepted and r.code.value == "payload_not_base64"
    assert gw.state_snapshot("d1")["record_count"] == 0


def test_malformed_frames_have_distinguishable_reasons() -> None:
    """残缺/非法报文必须被拒绝，且各类原因可区分。"""
    gw = make_gateway()
    cases = [
        ("不是 JSON 的文本", "{oops", "frame_not_json"),
        ("JSON 但非对象", "[1,2,3]", "frame_not_object"),
        ("缺少必填字段 device", {"v": 1, "type": "telemetry", "session": "s", "seq": 1,
                                  "frag": {"i": 0, "n": 1}, "payload_b64": "eA=="},
         "missing_required_field"),
        ("字段类型错误 seq=字符串", {"v": 1, "type": "telemetry", "device": "d1",
                                    "session": "s", "seq": "1",
                                    "frag": {"i": 0, "n": 1}, "payload_b64": "eA=="},
         "bad_field_type"),
        ("未知报文类型", {"v": 1, "type": "telegram", "device": "d1", "session": "s",
                         "seq": 1, "frag": {"i": 0, "n": 1}, "payload_b64": "eA=="},
         "unknown_message_type"),
        ("遥测缺少 frag", {"v": 1, "type": "telemetry", "device": "d1",
                          "session": "s", "seq": 1}, "missing_required_field"),
    ]
    for title, raw, expect in cases:
        r = gw.handle_frame(raw)
        dump_case(title, raw, r)
        assert not r.accepted, title
        assert r.code is not None and r.code.value == expect, (title, r.code)


def test_payload_json_and_required_validation() -> None:
    with pytest.raises(GatewayError) as ei:
        decode_payload(1, b"not-json")
    assert ei.value.code.value == "payload_not_json"

    with pytest.raises(GatewayError) as ei:
        decode_payload(1, json.dumps([1]).encode())
    assert ei.value.code.value == "payload_not_object"

    with pytest.raises(GatewayError) as ei:
        decode_payload(1, json.dumps({"ts": 1, "metrics": {}}).encode())
    assert ei.value.code.value == "bad_field_type"  # metrics 必须非空
