"""重复序列号去重与断线重连续传测试。"""

from __future__ import annotations

from gateway.core import Gateway
from gateway.registry import DeviceRegistry

from .helpers import ack_frame, dump_case, telemetry_frame


def make_gateway() -> Gateway:
    reg = DeviceRegistry()
    reg.register("d1", set())
    return Gateway(reg)


def payload(seq: int) -> dict:
    return {"ts": 1000 + seq, "metrics": {"v": seq}}


def test_duplicate_sequence_creates_no_extra_record() -> None:
    """同一序列号重复上报（含分片完整重放）只产生一条业务记录。"""
    gw = make_gateway()
    f1 = telemetry_frame(1, payload(1))
    r1 = gw.handle_frame(f1)
    dump_case("seq=1 首次上报", f1, r1, gw.state_snapshot("d1"))
    assert r1.accepted and not r1.duplicate

    f1_dup = telemetry_frame(1, payload(1))
    r2 = gw.handle_frame(f1_dup)
    dump_case("seq=1 完全重复上报", f1_dup, r2, gw.state_snapshot("d1"))
    assert r2.accepted and r2.duplicate and r2.record is None

    snap = gw.state_snapshot("d1")
    assert snap["record_count"] == 1
    assert snap["accepted_seqs"] == [1]


def test_consecutive_seqs_all_persisted_without_loss() -> None:
    gw = make_gateway()
    for seq in range(1, 6):
        r = gw.handle_frame(telemetry_frame(seq, payload(seq)))
        assert r.accepted and not r.duplicate
    snap = gw.state_snapshot("d1")
    dump_case("seq=1..5 顺序上报", "seq=1..5", {"accepted": True}, snap)
    assert snap["record_count"] == 5 and snap["accepted_seqs"] == [1, 2, 3, 4, 5]


def test_ack_advances_confirmed_point_and_duplicate_ack() -> None:
    gw = make_gateway()
    gw.handle_frame(telemetry_frame(1, payload(1)))
    a1 = ack_frame(1)
    r1 = gw.handle_frame(a1)
    dump_case("ACK seq=1 首次", a1, r1, gw.state_snapshot("d1"))
    assert r1.accepted and not r1.duplicate
    assert gw.state_snapshot("d1")["confirmed_seq"] == 1

    r2 = gw.handle_frame(ack_frame(1))
    assert r2.duplicate  # 重复 ACK 幂等，位点不回退
    assert gw.state_snapshot("d1")["confirmed_seq"] == 1


def test_resume_after_disconnect_no_loss_nor_duplicate() -> None:
    """断开 -> 新 session 携带正确位点重连：已确认数据保留，续传成功。"""
    gw = make_gateway()
    for seq in range(1, 4):
        gw.handle_frame(telemetry_frame(seq, payload(seq)))
    gw.handle_frame(ack_frame(2))  # 已确认到 2；seq=3 已收到但未确认

    gw.on_disconnect("d1")
    before = gw.state_snapshot("d1")
    assert before["online"] is False and before["record_count"] == 3

    # 以新 session 重连，resume_from=2（与网关已确认位点一致）
    f4 = telemetry_frame(4, payload(4), session="s2", resume_from=2)
    r = gw.handle_frame(f4)
    dump_case("断线后用 s2 携带 resume_from=2 续传 seq=4", f4, r, gw.state_snapshot("d1"))
    assert r.accepted and not r.duplicate
    snap = gw.state_snapshot("d1")
    assert snap["session_id"] == "s2"
    assert snap["record_count"] == 4  # 旧记录 1,2,3 未丢失，新增 4
    assert snap["accepted_seqs"] == [1, 2, 3, 4]

    # 续传期间重放旧 seq=3：仍去重，不产生第二条记录
    r_old = gw.handle_frame(telemetry_frame(3, payload(3), session="s2", resume_from=2))
    assert r_old.duplicate
    assert gw.state_snapshot("d1")["record_count"] == 4


def test_resume_without_point_is_rejected() -> None:
    """新会话不携带 resume_from：拒绝，防止绕过续传校验造成丢失/重复。"""
    gw = make_gateway()
    gw.handle_frame(telemetry_frame(1, payload(1)))
    gw.on_disconnect("d1")
    f = telemetry_frame(2, payload(2), session="s2")
    r = gw.handle_frame(f)
    dump_case("新会话缺少 resume_from", f, r)
    assert not r.accepted and r.code.value == "resume_from_mismatch"
    # 被拒绝后旧记录与位点保持不变（无状态回退）
    snap = gw.state_snapshot("d1")
    assert snap["session_id"] == "s1" and snap["record_count"] == 1


def test_resume_point_too_high_rejected_as_possible_loss() -> None:
    """设备声称的位点高于网关见过的数据：可能丢数据，明确拒绝。"""
    gw = make_gateway()
    gw.handle_frame(telemetry_frame(1, payload(1)))
    f = telemetry_frame(2, payload(2), session="s2", resume_from=9)
    r = gw.handle_frame(f)
    dump_case("resume_from=9 高于网关位点 1", f, r)
    assert not r.accepted and r.code.value == "resume_from_mismatch"


def test_resume_point_rollback_rejected() -> None:
    """设备声称位点低于网关已确认位点：状态回退，拒绝。"""
    gw = make_gateway()
    for seq in range(1, 4):
        gw.handle_frame(telemetry_frame(seq, payload(seq)))
    gw.handle_frame(ack_frame(3))
    f = telemetry_frame(4, payload(4), session="s2", resume_from=1)
    r = gw.handle_frame(f)
    dump_case("resume_from=1 低于已确认位点 3", f, r, gw.state_snapshot("d1"))
    assert not r.accepted and r.code.value == "resume_from_mismatch"
    assert gw.state_snapshot("d1")["confirmed_seq"] == 3  # 位点未被回退
