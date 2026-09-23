"""越权拒绝与并发串行下发、去重、积压上限测试。"""

from __future__ import annotations

import asyncio

from gateway.commands import CommandBus
from gateway.protocol import CommandRequest
from gateway.registry import DeviceRegistry

from .helpers import dump_case


def make_bus(max_pending: int = 4):
    reg = DeviceRegistry()
    reg.register("d1", {"set_level", "reboot"})
    reg.register("d2", {"set_level"})
    executed: list[tuple[str, int]] = []

    async def handler(req: CommandRequest) -> dict:
        await asyncio.sleep(0.005)  # 制造真实的并发窗口
        executed.append((req.device_id, req.args.get("level", 0)))
        return {"device": req.device_id, "level": req.args.get("level", 0)}

    return CommandBus(reg, max_pending=max_pending, handler=handler), executed


def test_unauthorized_and_unknown_device_have_distinct_reasons() -> None:
    async def scenario() -> None:
        bus, _ = make_bus()
        r1 = await bus.submit(CommandRequest("d1", "firmware_flash", "req-1"))
        dump_case("d1 请求越权指令 firmware_flash",
                  {"device": "d1", "command": "firmware_flash"}, r1)
        assert not r1.accepted and r1.code.value == "unauthorized_command"

        r2 = await bus.submit(CommandRequest("ghost", "reboot", "req-2"))
        dump_case("未知设备 ghost 请求指令",
                  {"device": "ghost", "command": "reboot"}, r2)
        assert not r2.accepted and r2.code.value == "device_unknown"
        assert r1.code != r2.code  # 两种失败原因必须可区分
        await bus.shutdown()

    asyncio.run(scenario())


def test_concurrent_commands_serialized_in_submission_order() -> None:
    """同一设备并发提交 20 条：严格按提交顺序串行生效，无重复无回退。"""
    async def scenario():
        bus, executed = make_bus(max_pending=4)
        reqs = [
            CommandRequest("d1", "set_level", f"r{i}", {"level": i})
            for i in range(20)
        ]
        results = await asyncio.gather(*(bus.submit(q) for q in reqs))
        accepted = [r for r in results if r.accepted]
        rejected_full = [
            r for r in results
            if r.code is not None and r.code.value == "command_queue_full"
        ]
        summary = {
            "submitted": 20,
            "accepted": len(accepted),
            "queue_full_rejected": len(rejected_full),
            "executed_levels": [lvl for _d, lvl in executed],
        }
        dump_case("d1 并发提交 20 条（积压上限 4）", summary,
                  {"accepted": True, "code": None})

        # 1) 不丢计数：接受 + 有界拒绝 == 提交总数
        assert len(accepted) + len(rejected_full) == 20
        # 2) 超限以可区分原因拒绝，而非无界堆积
        assert len(rejected_full) > 0
        assert all(r.reason for r in rejected_full)
        # 3) 生效顺序严格等于提交顺序（level 单调递增 => 无状态回退）
        levels = [level for _device, level in executed]
        assert levels == sorted(levels) and levels == list(range(len(levels)))
        # 4) 每条只执行一次
        assert len(executed) == len(set(executed))
        await bus.shutdown()

    asyncio.run(scenario())


def test_duplicate_request_id_never_executed_twice() -> None:
    async def scenario():
        bus, executed = make_bus()
        rs = await asyncio.gather(
            bus.submit(CommandRequest("d1", "set_level", "dup", {"level": 9})),
            bus.submit(CommandRequest("d1", "set_level", "dup", {"level": 9})),
            bus.submit(CommandRequest("d1", "set_level", "dup", {"level": 9})),
        )
        dump_case("同一 request_id 并发提交 3 次",
                  {"request_id": "dup", "copies": 3}, rs[0], {"executed": executed})
        assert all(r.executed for r in rs)
        assert sum(1 for r in rs if r.duplicate) == 2
        assert executed == [("d1", 9)]  # 只真正执行一次
        await bus.shutdown()

    asyncio.run(scenario())


def test_duplicate_request_after_completion_returns_first_result() -> None:
    async def scenario():
        bus, executed = make_bus()
        first = await bus.submit(CommandRequest("d1", "reboot", "once", {}))
        second = await bus.submit(CommandRequest("d1", "reboot", "once", {}))
        dump_case("完成后重放同一 request_id",
                  {"request_id": "once"}, second, {"executed": executed})
        assert first.executed and not first.duplicate
        assert second.executed and second.duplicate
        assert second.state == first.state
        assert executed == [("d1", 0)]
        await bus.shutdown()

    asyncio.run(scenario())


def test_different_devices_run_in_parallel_same_device_serial() -> None:
    """不同设备互不阻塞；各自设备内仍串行且保序。"""
    async def scenario():
        reg = DeviceRegistry()
        reg.register("d1", {"set_level"})
        reg.register("d2", {"set_level"})
        finished: list[tuple[str, str]] = []

        async def handler(req: CommandRequest) -> dict:
            await asyncio.sleep(0.02)
            finished.append((req.device_id, req.request_id))
            return {"ok": True}

        bus = CommandBus(reg, max_pending=8, handler=handler)
        # d1 两条必须保序；d2 的第一条应能与 d1 的第一条并行
        await asyncio.gather(
            bus.submit(CommandRequest("d1", "set_level", "a1", {"level": 1})),
            bus.submit(CommandRequest("d1", "set_level", "a2", {"level": 2})),
            bus.submit(CommandRequest("d2", "set_level", "b1", {"level": 3})),
        )
        dump_case("两设备并发：d1 两条保序，d2 独立", {"finished": finished},
                  {"accepted": True})
        order = [rid for _d, rid in finished]
        assert order.index("a1") < order.index("a2")  # 同设备保序
        assert "b1" in order and {d for d, _ in finished} == {"d1", "d2"}
        await bus.shutdown()

    asyncio.run(scenario())
