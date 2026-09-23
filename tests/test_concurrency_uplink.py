"""并发上行：多设备分片交错到达、多线程同时提交时不串号、不重复、不丢失。"""

from __future__ import annotations

import json
import threading

from gateway.core import Gateway
from gateway.registry import DeviceRegistry

from .helpers import dump_case, telemetry_frame


def test_interleaved_fragments_from_two_devices() -> None:
    """d1/d2 的分片交错送入重组器，各自还原各自的 payload。"""
    reg = DeviceRegistry()
    reg.register("d1", set())
    reg.register("d2", set())
    gw = Gateway(reg)

    p1 = json.dumps({"ts": 1, "metrics": {"who": "d1"}}).encode()
    p2 = json.dumps({"ts": 2, "metrics": {"who": "d2"}}).encode()
    c1, c2 = [p1[:4], p1[4:]], [p2[:5], p2[5:]]

    sequence = [
        telemetry_frame(1, device="d1", session="a", i=0, n=2, raw_payload=c1[0]),
        telemetry_frame(1, device="d2", session="b", i=1, n=2, raw_payload=c2[1]),
        telemetry_frame(1, device="d2", session="b", i=0, n=2, raw_payload=c2[0]),
        telemetry_frame(1, device="d1", session="a", i=1, n=2, raw_payload=c1[1]),
    ]
    results = [gw.handle_frame(f) for f in sequence]
    dump_case("两设备分片交错 4 帧", {"frames": "d1-0,d2-1,d2-0,d1-1"},
              {"completed_flags": [r.completed for r in results]},
              {"d1": gw.state_snapshot("d1"), "d2": gw.state_snapshot("d2")})

    assert gw.state_snapshot("d1")["record_count"] == 1
    assert gw.state_snapshot("d2")["record_count"] == 1
    # 内容未串号：重放 d1 seq=1 的任一分片都立即判重，不产生新记录
    r0 = gw.handle_frame(telemetry_frame(1, device="d1", session="a", i=0, n=2, raw_payload=c1[0]))
    r1 = gw.handle_frame(telemetry_frame(1, device="d1", session="a", i=1, n=2, raw_payload=c1[1]))
    assert r0.duplicate is True and r1.duplicate is True
    assert gw.reassembler.pending_count() == 0  # 重放分片不残留缓存
    assert gw.state_snapshot("d1")["record_count"] == 1


def test_concurrent_threads_same_device_no_duplicate_record() -> None:
    """多线程并发上报同一 seq 集合：每条 seq 恰好一条记录，无丢失。"""
    reg = DeviceRegistry()
    reg.register("d1", set())
    gw = Gateway(reg)
    errors: list[str] = []

    def worker(seq: int) -> None:
        for _ in range(20):  # 每个 seq 被两个线程各报 20 次
            r = gw.handle_frame(telemetry_frame(seq, {"ts": seq, "metrics": {"v": seq}}))
            if not r.accepted:
                errors.append(f"seq={seq} 被意外拒绝: {r.code}")

    threads = []
    for seq in range(1, 11):
        for _ in range(2):
            threads.append(threading.Thread(target=worker, args=(seq,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = gw.state_snapshot("d1")
    dump_case("10 个 seq × 2 线程 × 20 次重复上报",
              {"threads": len(threads), "attempts_per_seq": 40},
              {"accepted": not errors}, snap)
    assert not errors
    assert snap["record_count"] == 10
    assert snap["record_seqs"] == list(range(1, 11))
