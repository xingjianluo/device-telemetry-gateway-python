"""端到端综合场景：不稳定网络下分片/乱序/重复/重连，叠加并发下行。"""

from __future__ import annotations

import asyncio
import json

from gateway.core import Gateway
from gateway.protocol import CommandRequest
from gateway.registry import DeviceRegistry

from .helpers import dump_case, telemetry_frame


def build_gateway():
    reg = DeviceRegistry()
    reg.register("d1", {"set_level"})
    executed: list[tuple[str, int]] = []

    async def handler(req: CommandRequest) -> dict:
        await asyncio.sleep(0.002)
        executed.append((req.device_id, req.args.get("level", 0)))
        return {"last_level": req.args.get("level", 0)}

    return Gateway(reg, max_pending=4, command_handler=handler), executed


def test_unstable_network_uplink_is_consistent() -> None:
    """模拟：分片乱序 + 重复分片 + seq 重放 + 断线重连。

    最终断言：无重复业务记录、无丢失，失败原因均可区分。
    """
    gw, _ = build_gateway()
    log = []

    def step(title: str, frame: dict):
        r = gw.handle_frame(frame)
        log.append((title, r.to_dict()))
        dump_case(title, frame, r, gw.state_snapshot("d1"))
        return r

    # seq=1：单分片，随后整体重放两次
    p1 = {"ts": 100, "metrics": {"temp": 1}}
    step("seq=1 首次", telemetry_frame(1, p1))
    step("seq=1 网络重放(重复)", telemetry_frame(1, p1))

    # seq=2：3 个分片，乱序到达，且 i=0 重复到达一次（内容相同）
    full = json.dumps({"ts": 200, "metrics": {"temp": 2, "hum": 50}}).encode()
    chunks = [full[:6], full[6:14], full[14:]]
    step("seq=2 分片 i=2", telemetry_frame(2, i=2, n=3, raw_payload=chunks[2]))
    step("seq=2 分片 i=0", telemetry_frame(2, i=0, n=3, raw_payload=chunks[0]))
    step("seq=2 分片 i=0 重复", telemetry_frame(2, i=0, n=3, raw_payload=chunks[0]))
    r = step("seq=2 分片 i=1（集齐）", telemetry_frame(2, i=1, n=3, raw_payload=chunks[1]))
    assert r.record["payload"]["metrics"] == {"temp": 2, "hum": 50}

    # seq=3 残缺：只来一片就断线，重连后旧的未完成分片作废，需重发
    step("seq=3 断线前仅到 i=0", telemetry_frame(3, i=0, n=2, raw_payload=chunks[0]))
    gw.on_disconnect("d1")
    gw.handle_frame({"v": 1, "type": "ack", "device": "d1", "session": "s1", "seq": 2})

    # 重连：新 session + 正确位点；seq=3 重新完整发送
    p3 = {"ts": 300, "metrics": {"temp": 3}}
    f3 = telemetry_frame(3, p3, session="s2", resume_from=2)
    r3 = step("s2 续传 seq=3（重发完整报文）", f3)
    assert r3.accepted and not r3.duplicate

    # 坏报文：坏 base64 / 越版本，必须被拒绝且原因不同，且不产生记录
    bad1 = telemetry_frame(4, raw_payload=b"x", session="s2")
    bad1["payload_b64"] = "!!!!"
    rb1 = step("s2 seq=4 坏 base64", bad1)
    assert rb1.code.value == "payload_not_base64"
    bad2 = telemetry_frame(5, p3, session="s2", version=77)
    rb2 = step("s2 seq=5 版本不支持", bad2)
    assert rb2.code.value == "unsupported_version"
    assert rb1.code != rb2.code

    snap = gw.state_snapshot("d1")
    print("\n最终汇聚状态:", json.dumps(snap, ensure_ascii=False, sort_keys=True))
    assert snap["confirmed_seq"] == 2
    assert snap["record_seqs"] == [1, 2, 3]       # 无丢失
    assert snap["record_count"] == 3              # 无重复
    # 每条记录内容正确
    assert snap["session_id"] == "s2"


def test_uplink_and_concurrent_downlink_together() -> None:
    """上行去重的同时并发下发指令：串行生效、越权可区分、积压可区分。"""
    gw, executed = build_gateway()

    async def scenario():
        # 先有上行数据
        for seq in range(1, 4):
            gw.handle_frame(telemetry_frame(seq, {"ts": seq, "metrics": {"v": seq}}))

        reqs = [CommandRequest("d1", "set_level", f"c{i}", {"level": i}) for i in range(12)]
        results = await asyncio.gather(*(gw.send_command(q) for q in reqs))
        # 越权
        denied = await gw.send_command(CommandRequest("d1", "firmware_flash", "cX"))
        await gw.command_bus.shutdown()
        return results, denied

    results, denied = asyncio.run(scenario())
    full = [r for r in results if r.code is not None
            and r.code.value == "command_queue_full"]
    accepted = [r for r in results if r.accepted]
    dump_case(
        "12 条并发指令 + 1 条越权",
        {"submitted": 12, "unauthorized": 1},
        {"accepted_count": len(accepted), "queue_full": len(full),
         "denied_code": denied.code.value},
        {"executed_levels": [lvl for _d, lvl in executed],
         "uplink_records": gw.state_snapshot("d1")["record_seqs"]},
    )
    assert not denied.accepted and denied.code.value == "unauthorized_command"
    assert len(accepted) + len(full) == 12
    assert len(full) > 0
    levels = [lvl for _d, lvl in executed]
    assert levels == sorted(levels)          # 保序、无回退
    assert len(levels) == len(set(levels))   # 无重复执行
    # 上行记录不受下行流量影响
    assert gw.state_snapshot("d1")["record_seqs"] == [1, 2, 3]
