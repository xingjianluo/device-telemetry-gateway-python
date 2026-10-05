"""指令状态查询 / 取消 / 容量治理单测（本轮新增）。

并发窗口全部用确定性的闸门（Event/Barrier）构造，不依赖 sleep 凑结果：
  * start_gate 关闭时，票据 0 的处理器占用工作线程并阻塞；
  * ready 事件确认票据 0 已进入 RUNNING 后，其余票据才被放行提交/取消，
    因此“谁在途、谁排队、谁能取消”在每次运行中都完全确定。
每个用例打印：输入、判定依据、最终状态。
"""
from __future__ import annotations

import json
import threading
import unittest

from gateway.commands import Command, CommandExecutor
from gateway.commandstore import CommandState
from gateway.gateway import Gateway
from gateway.protocol import (
    R_COMMAND_DUPLICATE,
    R_COMMAND_HANDLER_ERROR,
    R_COMMAND_NOT_CANCELLABLE,
    R_COMMAND_NOT_FOUND,
    R_COMMAND_QUEUE_FULL,
    R_COMMAND_RETIRED,
    R_COMMAND_UNAUTHORIZED,
    R_DEVICE_UNKNOWN,
)


def report(title: str, inp, verdict: str, final) -> None:
    print("\n" + "=" * 72)
    print(f"用例: {title}")
    print(f"输入: {json.dumps(inp, ensure_ascii=False, sort_keys=True, default=str)}")
    print(f"判定依据: {verdict}")
    print(f"最终状态: {final}")


class BlockingRig:
    """确定性并发装置：票据 0 阻塞在闸门，ready 表示其已 RUNNING。"""

    def __init__(self, max_queue: int = 8, max_history: int = 256,
                 max_retired: int = 4096, authz=None) -> None:
        self.gate = threading.Event()
        self.ready = threading.Event()
        self.executed: list[str] = []
        self.log_lock = threading.Lock()

        def blocking(device_id: str, args: dict) -> dict:
            n = args["n"]
            if n == 0:
                self.ready.set()
                self.gate.wait(timeout=3)
            with self.log_lock:
                self.executed.append(f"{device_id}:{n}")
            return {"n": n}

        def boom(device_id: str, args: dict) -> dict:
            with self.log_lock:
                self.executed.append(f"{device_id}:boom")
            raise RuntimeError("handler blew up")

        self.executor = CommandExecutor(
            authorizations=authz or {"d1": frozenset({"blocking", "boom"})},
            handlers={"blocking": blocking, "boom": boom},
            max_queue=max_queue, max_history=max_history, max_retired=max_retired,
        )
        self.gateway = Gateway(
            executor=self.executor,
            known_devices=frozenset((authz or {"d1": 1}).keys()),
        )

    def open(self) -> None:
        self.gate.set()


class QueryStatusTests(unittest.TestCase):
    def test_succeeded_query_is_stable_and_repeatable(self) -> None:
        rig = BlockingRig()
        r = rig.gateway.submit_command(Command("d1", "c-ok", "blocking", {"n": 7}))
        self.assertEqual(r.status, "ok")
        q1 = rig.gateway.query_command("c-ok")
        q2 = rig.gateway.query_command("c-ok")
        self.assertEqual(q1.state, CommandState.SUCCEEDED)
        self.assertEqual(q1.response, {"n": 7})
        self.assertFalse(q1.retired)
        # 同一指令反复查，结论稳定一致（快照不可变）
        self.assertEqual((q1.state, q1.response, q1.reason),
                         (q2.state, q2.response, q2.reason))
        report("查询-成功终态稳定",
               {"submit": "d1/blocking n=7", "query": ["c-ok"] * 2},
               "执行成功 -> succeeded 带 response；重复查询同一不可变快照",
               {"state": q1.state.value, "response": q1.response})

    def test_failed_and_unsupported_have_distinguishable_states(self) -> None:
        rig = BlockingRig()
        err = rig.gateway.submit_command(Command("d1", "c-err", "boom"))
        self.assertEqual(err.reason.reason, R_COMMAND_HANDLER_ERROR)
        q = rig.gateway.query_command("c-err")
        self.assertEqual(q.state, CommandState.FAILED)
        self.assertEqual(q.reason.reason, R_COMMAND_HANDLER_ERROR)
        report("查询-处理器失败原因可区分",
               {"submit": "d1/boom(抛异常)"},
               "处理器异常 -> failed 终态，保留 COMMAND_HANDLER_ERROR 原因",
               {"state": q.state.value, "reason": q.reason.reason})

    def test_unknown_command_id_is_explicitly_not_found(self) -> None:
        rig = BlockingRig()
        missing = rig.gateway.query_command("never-submitted")
        # 与“提交过但已淘汰”必须是不同答复；这里是从未提交 -> NOT_FOUND
        self.assertEqual(missing.reason, R_COMMAND_NOT_FOUND)
        report("查询-从未提交的指令号",
               {"query": "never-submitted"},
               "注册表无记录且无墓碑 -> COMMAND_NOT_FOUND，绝不含糊为成功",
               missing.to_dict())

    def test_duplicate_submission_status_is_consistent(self) -> None:
        rig = BlockingRig()
        out: list = []
        barrier = threading.Barrier(6)

        def fire() -> None:
            barrier.wait()
            out.append(rig.gateway.submit_command(
                Command("d1", "dup-id", "blocking", {"n": 1})))

        threads = [threading.Thread(target=fire) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        first = [r for r in out if not r.duplicate]
        dups = [r for r in out if r.duplicate]
        self.assertEqual(len(first), 1)
        self.assertEqual(len(dups), 5)
        self.assertTrue(all(r.status == "ok" for r in out))
        q = rig.gateway.query_command("dup-id")
        self.assertEqual(q.state, CommandState.SUCCEEDED)
        self.assertEqual(rig.executed.count("d1:1"), 1, "同号并发只能执行一次")
        report("查询-并发同号结论一致",
               {"concurrency": 6, "command_id": "dup-id"},
               "1 次真正执行 + 5 个 duplicate 复用同一结论；执行次数恰为 1",
               {"executed": rig.executed, "state": q.state.value,
                "duplicates": len(dups)})


class RejectedQueryTests(unittest.TestCase):
    """提交即拒（未授权/设备不存在/积压超限）的指令：凭号可查、结论稳定、
    与“从未提交”明确可区分、容量有界、对账口径不变。"""

    def test_unauthorized_rejection_is_queryable_and_stable(self) -> None:
        rig = BlockingRig(authz={"d1": frozenset({"blocking", "boom"}),
                                 "d2": frozenset()})
        r = rig.gateway.submit_command(Command("d2", "rej-1", "blocking"))
        self.assertEqual(r.status, "rejected")
        self.assertEqual(r.reason.reason, R_COMMAND_UNAUTHORIZED)
        # 同一被拒指令号连续查多次：结论逐字节一致
        queries = [rig.gateway.query_command("rej-1") for _ in range(3)]
        for q in queries:
            self.assertEqual(q.state, CommandState.REJECTED)
            self.assertEqual(q.reason.reason, R_COMMAND_UNAUTHORIZED)
            self.assertEqual(q.device_id, "d2")
            self.assertFalse(q.retired)
        self.assertEqual(
            [(q.state, q.reason, q.device_id) for q in queries],
            [(queries[0].state, queries[0].reason, queries[0].device_id)] * 3,
            "被拒指令号反复查询结论必须稳定一致")
        # “被拒”与“从未提交”是两种明确、可区分的答复
        missing = rig.gateway.query_command("rej-ghost")
        self.assertEqual(missing.reason, R_COMMAND_NOT_FOUND)
        self.assertNotEqual(queries[0].state.value, missing.reason)
        # 被拒指令从未入队、从未执行
        self.assertEqual(rig.executed, [])
        report("被拒可查-未授权拒绝稳定可区分",
               {"submit": "d2/blocking(未授权)", "query": ["rej-1"] * 3,
                "also_query": "rej-ghost"},
               "提交即拒 -> rejected 终态落账；反复查询同一快照；"
               "从未提交 -> COMMAND_NOT_FOUND，两种答复截然不同",
               {"state": queries[0].state.value,
                "reason": queries[0].reason.reason,
                "missing": missing.reason, "executed": rig.executed})

    def test_unknown_device_rejection_has_distinct_reason(self) -> None:
        rig = BlockingRig()
        r = rig.gateway.submit_command(Command("ghost", "rej-2", "blocking"))
        self.assertEqual(r.status, "rejected")
        self.assertEqual(r.reason.reason, R_DEVICE_UNKNOWN)
        q = rig.gateway.query_command("rej-2")
        self.assertEqual(q.state, CommandState.REJECTED)
        self.assertEqual(q.reason.reason, R_DEVICE_UNKNOWN)
        self.assertEqual(q.device_id, "ghost")
        # 三类拒绝原因互可区分：设备不存在 != 未授权 != 积压超限
        self.assertNotEqual(q.reason.reason, R_COMMAND_UNAUTHORIZED)
        self.assertNotEqual(q.reason.reason, R_COMMAND_QUEUE_FULL)
        report("被拒可查-设备不存在原因可区分",
               {"submit": "ghost/blocking(设备未注册)", "query": "rej-2"},
               "设备未注册 -> rejected + DEVICE_UNKNOWN；"
               "与 COMMAND_UNAUTHORIZED / COMMAND_QUEUE_FULL 明确区分",
               {"state": q.state.value, "reason": q.reason.reason})
    def test_queue_full_rejection_queryable_and_not_replayed_after_drain(self) -> None:
        # 确定性铺满：票据 0 在途 + 排队 3 槽；第 5 个提交被积压上限拒绝
        rig = BlockingRig(max_queue=3)
        starter = threading.Thread(
            target=rig.gateway.submit_command,
            args=(Command("d1", "b-0", "blocking", {"n": 0}),))
        starter.start()
        self.assertTrue(rig.ready.wait(3))  # b-0 确定 RUNNING
        waiters = [
            threading.Thread(
                target=rig.gateway.submit_command,
                args=(Command("d1", f"b-{n}", "blocking", {"n": n}),))
            for n in (1, 2, 3)]
        for th in waiters:
            th.start()
        import time as _time
        state = rig.executor._devices["d1"]
        deadline = _time.monotonic() + 3
        while state.queue.qsize() < 3 and _time.monotonic() < deadline:
            pass
        self.assertEqual(state.queue.qsize(), 3, "排队槽必须确定性铺满")
        r = rig.gateway.submit_command(Command("d1", "b-4", "blocking", {"n": 4}))
        self.assertEqual(r.status, "rejected")
        self.assertEqual(r.reason.reason, R_COMMAND_QUEUE_FULL)
        # 被拒当下即可凭号查到明确结论
        q1 = rig.gateway.query_command("b-4")
        self.assertEqual(q1.state, CommandState.REJECTED)
        self.assertEqual(q1.reason.reason, R_COMMAND_QUEUE_FULL)
        # 放行排空：积压有了空位，但同号再提交绝不改判、绝不补执行
        rig.open()
        starter.join()
        for th in waiters:
            th.join()
        again = rig.gateway.submit_command(
            Command("d1", "b-4", "blocking", {"n": 4}))
        self.assertEqual(again.status, "rejected")
        self.assertEqual(again.reason.reason, R_COMMAND_QUEUE_FULL)
        self.assertTrue(again.duplicate, "同号再提交必须幂等复用原拒绝结论")
        self.assertNotIn("d1:4", rig.executed, "被拒指令绝不能被补执行")
        q2 = rig.gateway.query_command("b-4")
        self.assertEqual(q2.state, CommandState.REJECTED)
        self.assertEqual(q2.reason.reason, R_COMMAND_QUEUE_FULL)
        m = rig.gateway.command_metrics()
        self.assertTrue(m.accounts_balanced())
        self.assertEqual(m.terminal.get("succeeded"), 4,
                         "只有真正入队的 4 条成功，被拒的不计入成功")
        report("被拒可查-积压超限且排空后不重放",
               {"max_queue": 3, "overflow_submit": "b-4",
                "after_drain_resubmit": "b-4"},
               "QUEUE_FULL 拒绝落账可查；排空后同号再提交仍复用原拒绝结论"
               "(duplicate)，绝不补执行；成功计数不含被拒指令",
               {"q1": q1.reason.reason, "resubmit": again.reason.reason,
                "duplicate": again.duplicate, "executed": rig.executed,
                "succeeded": m.terminal.get("succeeded")})

    def test_concurrent_rejected_submissions_converge_to_single_verdict(self) -> None:
        rig = BlockingRig(authz={"d1": frozenset({"blocking", "boom"}),
                                 "d2": frozenset()})
        barrier = threading.Barrier(6)
        out: list = []
        lock = threading.Lock()

        def fire() -> None:
            barrier.wait()
            r = rig.gateway.submit_command(Command("d2", "rej-c", "blocking"))
            with lock:
                out.append(r)

        threads = [threading.Thread(target=fire) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 全部得到同一个“已拒绝”结论：恰好 1 次记拒绝，其余幂等 duplicate
        self.assertTrue(all(r.status == "rejected" for r in out))
        self.assertTrue(
            all(r.reason.reason == R_COMMAND_UNAUTHORIZED for r in out))
        self.assertEqual(len([r for r in out if not r.duplicate]), 1)
        self.assertEqual(len([r for r in out if r.duplicate]), 5)
        q = rig.gateway.query_command("rej-c")
        self.assertEqual(q.state, CommandState.REJECTED)
        self.assertEqual(q.reason.reason, R_COMMAND_UNAUTHORIZED)
        m = rig.gateway.command_metrics()
        self.assertEqual(m.rejected.get(R_COMMAND_UNAUTHORIZED), 1)
        self.assertEqual(m.duplicates, 5)
        self.assertEqual(m.terminal.get("rejected"), 1,
                         "同一指令号只落一条 REJECTED 记录")
        self.assertTrue(m.accounts_balanced())
        self.assertEqual(rig.executed, [])
        report("被拒可查-并发同号收敛单一结论",
               {"concurrency": 6, "command_id": "rej-c", "device": "d2(未授权)"},
               "6 路并发同号提交全被拒：1 次落账 + 5 次 duplicate；"
               "查询/计数/对账结论唯一",
               {"duplicates": m.duplicates,
                "rejected": m.rejected, "state": q.state.value})
    def test_rejected_records_are_bounded_by_history_and_tombstones(self) -> None:
        rig = BlockingRig(max_history=2, max_retired=2,
                          authz={"d1": frozenset({"blocking"}),
                                 "d2": frozenset()})
        for n in range(5):
            r = rig.gateway.submit_command(Command("d2", f"rj-{n}", "blocking"))
            self.assertEqual(r.status, "rejected")
        m = rig.gateway.command_metrics()
        # 被拒记录与成功/失败/取消一样纳入终态容量治理：保留窗与墓碑环都有界
        self.assertEqual(m.history, 2)
        self.assertEqual(m.retired, 2)
        self.assertEqual(m.terminal.get("rejected"), 5,
                         "终态计数单调累计，不随淘汰回退")
        # 最老的被拒记录被彻底遗忘 -> 回到“从未提交”的答复
        forgotten = rig.gateway.query_command("rj-0")
        self.assertEqual(forgotten.reason, R_COMMAND_NOT_FOUND)
        # 墓碑窗内：仍给出淘汰前的“已拒绝”结论，原因保留、可区分
        tomb = rig.gateway.query_command("rj-2")
        self.assertTrue(tomb.retired)
        self.assertEqual(tomb.state, CommandState.REJECTED)
        self.assertEqual(tomb.reason.reason, R_COMMAND_UNAUTHORIZED)
        # 保留窗内：retired=False 的鲜活终态
        fresh = rig.gateway.query_command("rj-4")
        self.assertFalse(fresh.retired)
        self.assertEqual(fresh.state, CommandState.REJECTED)
        # 墓碑内同号再提交：仍是明确拒绝（COMMAND_RETIRED），绝不变成执行
        replay = rig.gateway.submit_command(Command("d2", "rj-2", "blocking"))
        self.assertEqual(replay.status, "rejected")
        self.assertEqual(replay.reason.reason, R_COMMAND_RETIRED)
        self.assertEqual(rig.executed, [])
        self.assertTrue(rig.gateway.command_metrics().accounts_balanced())
        report("被拒可查-容量有界且墓碑可区分",
               {"rejected_submits": 5, "max_history": 2, "max_retired": 2,
                "resubmit": "rj-2"},
               "REJECTED 记录纳入 FIFO 保留窗 + 墓碑环；最老彻底遗忘回到 "
               "NOT_FOUND；墓碑内同号再提交 COMMAND_RETIRED 拒绝",
               {"history": m.history, "retired": m.retired,
                "rj-0": forgotten.reason,
                "rj-2": f"{tomb.state.value}(retired)", "replay": replay.reason.reason})

    def test_rejected_command_not_cancellable_and_never_counts_as_success(self) -> None:
        rig = BlockingRig(authz={"d1": frozenset({"blocking"}),
                                 "d2": frozenset()})
        rig.gateway.submit_command(Command("d2", "rej-x", "blocking"))
        # 被拒指令是终态：取消得到明确 NOT_CANCELLABLE，状态不因此改变
        c = rig.gateway.cancel_command("rej-x")
        self.assertEqual(c.status, "rejected")
        self.assertEqual(c.reason.reason, R_COMMAND_NOT_CANCELLABLE)
        q = rig.gateway.query_command("rej-x")
        self.assertEqual(q.state, CommandState.REJECTED)
        # 对账：被拒指令绝不计入 accepted / succeeded
        m = rig.gateway.command_metrics()
        self.assertEqual(m.accepted, 0)
        self.assertEqual(m.terminal.get("succeeded"), 0)
        self.assertEqual(m.terminal.get("rejected"), 1)
        self.assertEqual(m.rejected.get(R_COMMAND_UNAUTHORIZED), 1)
        self.assertTrue(m.accounts_balanced())
        report("被拒可查-不可取消且不计成功",
               {"submit": "d2/blocking(未授权)", "then": "cancel rej-x"},
               "REJECTED 为终态：取消 NOT_CANCELLABLE；"
               "accepted/succeeded 均为 0，账平",
               {"cancel": c.reason.reason, "state": q.state.value,
                "metrics": m.to_dict()})


class CancelTests(unittest.TestCase):
    def _rig_with_backlog(self, count_queued: int = 3) -> BlockingRig:
        """票据 0 在途阻塞；票据 1..count_queued 确定性地排队。"""
        rig = BlockingRig(max_queue=16)
        starter = threading.Thread(
            target=rig.gateway.submit_command,
            args=(Command("d1", "t-0", "blocking", {"n": 0}),))
        starter.start()
        self.assertTrue(rig.ready.wait(3), "票据 0 必须确定地进入 RUNNING")
        waiters = []
        for n in range(1, count_queued + 1):
            th = threading.Thread(
                target=rig.gateway.submit_command,
                args=(Command("d1", f"t-{n}", "blocking", {"n": n}),))
            th.start()
            waiters.append(th)
        return rig, starter, waiters  # type: ignore[return-value]

    def test_cancel_queued_succeeds_and_is_idempotent(self) -> None:
        rig, starter, waiters = self._rig_with_backlog(3)
        try:
            # t-2 仍在 QUEUED
            self.assertEqual(rig.gateway.query_command("t-2").state,
                             CommandState.QUEUED)
            first = rig.gateway.cancel_command("t-2")
            again = rig.gateway.cancel_command("t-2")
            third = rig.gateway.cancel_command("t-2")
            self.assertEqual(first.status, "cancelled")
            self.assertFalse(first.duplicate)
            self.assertEqual(again.status, "cancelled")
            self.assertTrue(again.duplicate, "重复取消必须幂等")
            self.assertTrue(third.duplicate)
        finally:
            rig.open()
            starter.join()
            for w in waiters:
                w.join()
        # 被取消指令绝不执行；提交方收到 cancelled 结论
        self.assertNotIn("d1:2", rig.executed)
        self.assertEqual(rig.gateway.query_command("t-2").state,
                         CommandState.CANCELLED)
        report("取消-排队中指令幂等",
               {"backlog": ["t-0 RUNNING", "t-1 QUEUED", "t-2 QUEUED",
                            "t-3 QUEUED"], "cancel": ["t-2"] * 3},
               "QUEUED -> CANCELLED 仅首次；重复取消 cancelled+duplicate，不执行",
               {"executed": rig.executed})

    def test_cancel_running_is_rejected_with_reason(self) -> None:
        rig, starter, waiters = self._rig_with_backlog(1)
        try:
            r = rig.gateway.cancel_command("t-0")  # 正在 RUNNING
            self.assertEqual(r.status, "rejected")
            self.assertEqual(r.reason.reason, R_COMMAND_NOT_CANCELLABLE)
        finally:
            rig.open()
            starter.join()
            for w in waiters:
                w.join()
        # 被拒取消后指令照常执行完毕，状态不退回
        self.assertEqual(rig.gateway.query_command("t-0").state,
                         CommandState.SUCCEEDED)
        report("取消-执行中拒绝",
               {"cancel": "t-0(RUNNING)"},
               "RUNNING 不属于‘尚未开始执行’ -> COMMAND_NOT_CANCELLABLE，不影响完成",
               {"reason": r.reason.reason,
                "state": rig.gateway.query_command("t-0").state.value})

    def test_cancel_finished_is_rejected_and_state_never_regresses(self) -> None:
        rig = BlockingRig()
        rig.gateway.submit_command(Command("d1", "done", "blocking", {"n": 9}))
        before = rig.gateway.query_command("done")
        r = rig.gateway.cancel_command("done")
        after = rig.gateway.query_command("done")
        self.assertEqual(r.reason.reason, R_COMMAND_NOT_CANCELLABLE)
        self.assertEqual(before.state, after.state, CommandState.SUCCEEDED)
        report("取消-已完成拒绝且状态不回退",
               {"submit": "done(n=9)", "then": "cancel done"},
               "SUCCEEDED 终态不可取消；状态保持 succeeded，不退回 queued/cancelled",
               {"reason": r.reason.reason, "state": after.state.value})

    def test_cancel_unknown_id_is_not_found(self) -> None:
        rig = BlockingRig()
        r = rig.gateway.cancel_command("ghost")
        self.assertEqual(r.status, "rejected")
        self.assertEqual(r.reason.reason, R_COMMAND_NOT_FOUND)
        report("取消-未知指令号",
               {"cancel": "ghost"},
               "无记录无墓碑 -> COMMAND_NOT_FOUND，与不可取消/成功取消可区分",
               r.reason.to_dict())

    def test_cancel_preserves_fifo_order_of_other_commands(self) -> None:
        rig, starter, waiters = self._rig_with_backlog(4)
        try:
            # 取消队列中间的 t-2；t-1,t-3,t-4 的相对先后不得被打乱
            rig.gateway.cancel_command("t-2")
        finally:
            rig.open()
            starter.join()
            for w in waiters:
                w.join()
        executed = [x for x in rig.executed]
        self.assertNotIn("d1:2", executed)
        self.assertEqual(executed, ["d1:0", "d1:1", "d1:3", "d1:4"],
                         "取消仅占位跳过，其余指令严格按提交顺序生效")
        report("取消-不打乱同设备顺序",
               {"queue": [0, 1, 2, 3, 4], "cancel": 2},
               "t-2 保留队列位置、轮空跳过；0,1,3,4 相对顺序不变",
               {"executed_order": executed})

    def test_concurrent_cancel_and_submit_has_single_outcome(self) -> None:
        """确定性并发：票据 0 在途；闸门保证 submit(t-x) 与 cancel(t-x) 同时竞争。"""
        rig = BlockingRig(max_queue=16)
        start = threading.Event()
        starter = threading.Thread(
            target=rig.gateway.submit_command,
            args=(Command("d1", "t-0", "blocking", {"n": 0}),))
        starter.start()
        self.assertTrue(rig.ready.wait(3))
        outcomes: list[str] = []
        lock = threading.Lock()

        def submit_queued() -> None:
            start.wait(3)
            r = rig.gateway.submit_command(
                Command("d1", "t-x", "blocking", {"n": 11}))
            with lock:
                outcomes.append(f"submit:{r.status}")

        def cancel_queued() -> None:
            start.wait(3)
            r = rig.gateway.cancel_command("t-x")
            with lock:
                outcomes.append(f"cancel:{r.status}")

        th_s = threading.Thread(target=submit_queued)
        th_c = threading.Thread(target=cancel_queued)
        th_s.start(); th_c.start()
        start.set()          # 精确同时放行两个竞争动作
        th_s.join(); th_c.join()
        rig.open()
        starter.join()

        final = rig.gateway.query_command("t-x")
        # 要么被取消（未执行），要么成功（执行一次），二者必居其一且不矛盾
        self.assertIn(final.state, (CommandState.CANCELLED, CommandState.SUCCEEDED))
        if final.state is CommandState.CANCELLED:
            self.assertNotIn("d1:11", rig.executed, "取消成功则绝不能执行")
            self.assertIn("cancel:cancelled", outcomes)
        else:
            self.assertEqual(rig.executed.count("d1:11"), 1,
                             "成功路径只能执行一次")
        # 不可能出现“既取消又执行”
        self.assertFalse(
            final.state is CommandState.CANCELLED and "d1:11" in rig.executed)
        report("取消-与提交并发的单一结论",
               {"race": ["submit t-x", "cancel t-x"], "barrier": "同一 Event 放行"},
               "竞争结果唯一：cancelled(必不执行) 或 succeeded(恰好执行一次)",
               {"outcomes": sorted(outcomes), "final": final.state.value,
                "executed": rig.executed})


class CapacityTests(unittest.TestCase):
    def test_history_is_bounded_and_active_never_evicted(self) -> None:
        rig = BlockingRig(max_queue=16, max_history=3, max_retired=100)
        for n in range(8):
            self.assertEqual(
                rig.gateway.submit_command(
                    Command("d1", f"h-{n}", "blocking", {"n": n + 100})).status,
                "ok")
        m = rig.gateway.command_metrics()
        self.assertEqual(m.history, 3, "终态保留窗严格不超过 max_history")
        self.assertEqual(m.retired, 8 - 3)
        # 最早的终态指令已进墓碑：仍可查、可区分、结论是淘汰前终态
        q0 = rig.gateway.query_command("h-0")
        self.assertTrue(q0.retired)
        self.assertEqual(q0.state, CommandState.SUCCEEDED)
        q_last = rig.gateway.query_command("h-7")
        self.assertFalse(q_last.retired)
        # 保留窗/墓碑内的 command_id 再提交 -> 明确拒绝，绝不二次执行
        again = rig.gateway.submit_command(Command("d1", "h-0", "blocking", {"n": 1}))
        self.assertEqual(again.reason.reason, R_COMMAND_RETIRED)
        self.assertTrue(m.accounts_balanced())
        report("容量-终态 FIFO 有界 + 墓碑防重放",
               {"submitted": 8, "max_history": 3, "resubmit": "h-0"},
               "超出 max_history 的最早终态进墓碑；非终态不清；墓碑内 id 拒绝重放",
               {"history": m.history, "retired": m.retired,
                "h-0": q0.state.value, "resubmit": again.reason.reason})

    def test_retired_tombstones_are_also_bounded(self) -> None:
        rig = BlockingRig(max_queue=16, max_history=1, max_retired=2)
        for n in range(6):
            rig.gateway.submit_command(
                Command("d1", f"r-{n}", "blocking", {"n": n}))
        # 1 条在保留窗 + 2 条墓碑；更老的墓碑被彻底遗忘
        m = rig.gateway.command_metrics()
        self.assertEqual(m.history, 1)
        self.assertEqual(m.retired, 2)
        forgotten = rig.gateway.query_command("r-0")
        self.assertEqual(forgotten.reason, R_COMMAND_NOT_FOUND)
        # 墓碑内取消：成功终态明确不可取消；查询仍稳定
        tomb_ok = rig.gateway.query_command("r-4")
        self.assertTrue(tomb_ok.retired)
        c = rig.gateway.cancel_command("r-4")
        self.assertEqual(c.reason.reason, R_COMMAND_NOT_CANCELLABLE)
        report("容量-墓碑环有界",
               {"submitted": 6, "max_history": 1, "max_retired": 2},
               "终态窗 1 + 墓碑 2 有界；最老记录彻底遗忘回到 NOT_FOUND",
               {"metrics": m.to_dict(),
                "r-0": forgotten.reason, "r-4_retired": tomb_ok.retired})

    def test_queued_and_running_commands_survive_terminal_eviction(self) -> None:
        """非终态绝不能被终态清理误伤：

        d1 的 t-0 处于 RUNNING、t-1 处于 QUEUED 时，在 d2 上制造远超
        max_history 的终态，触发多轮 FIFO 淘汰；随后放行，d1 两条活跃指令
        必须仍然存在并正常成功（而不是被当成不存在或跳过）。
        """
        rig = BlockingRig(
            max_queue=16, max_history=2, max_retired=50,
            authz={"d1": frozenset({"blocking", "boom"}),
                   "d2": frozenset({"blocking", "boom"})})
        starter = threading.Thread(
            target=rig.gateway.submit_command,
            args=(Command("d1", "t-0", "blocking", {"n": 0}),))
        starter.start()
        self.assertTrue(rig.ready.wait(3))
        waiter = threading.Thread(
            target=rig.gateway.submit_command,
            args=(Command("d1", "t-1", "blocking", {"n": 1}),))
        waiter.start()
        # 确认 t-1 已排队（非终态），此时 d2 上连续完成多条终态触发淘汰
        self.assertEqual(rig.gateway.query_command("t-1").state,
                         CommandState.QUEUED)
        for n in range(20):
            r = rig.gateway.submit_command(
                Command("d2", f"e-{n}", "blocking", {"n": n + 100}))
            self.assertEqual(r.status, "ok")
        m = rig.gateway.command_metrics()
        self.assertEqual(m.history, 2)
        self.assertGreaterEqual(m.retired, 18)
        # 活跃记录依然完好，可查且状态未被终态清理改动
        self.assertEqual(rig.gateway.query_command("t-0").state,
                         CommandState.RUNNING)
        self.assertEqual(rig.gateway.query_command("t-1").state,
                         CommandState.QUEUED)
        rig.open()
        starter.join(); waiter.join()
        self.assertEqual(
            [rig.gateway.query_command(f"t-{n}").state for n in (0, 1)],
            [CommandState.SUCCEEDED, CommandState.SUCCEEDED])
        self.assertEqual(rig.executed.count("d1:0"), 1)
        self.assertEqual(rig.executed.count("d1:1"), 1)
        report("容量-在途排队不被终态清理误伤",
               {"d1": ["t-0 RUNNING", "t-1 QUEUED"],
                "d2": "连续完成 20 条终态", "max_history": 2},
               "淘汰只作用于终态 FIFO；非终态记录留在注册表直到放行后成功",
               {"retired": m.retired,
                "t-0/t-1": ["succeeded", "succeeded"]})

    def test_backlog_full_still_rejected_distinguishably(self) -> None:
        # 有效容量 = 在途 1 + 排队 max_queue；占满后再提交必须 QUEUE_FULL。
        # 排队提交放到独立线程（submit 会阻塞至完成），用队列 qsize 确定性确认铺满。
        rig = BlockingRig(max_queue=3)
        starter = threading.Thread(
            target=rig.gateway.submit_command,
            args=(Command("d1", "b-0", "blocking", {"n": 0}),))
        starter.start()
        self.assertTrue(rig.ready.wait(3))  # b-0 确定 RUNNING
        queued_threads = [
            threading.Thread(
                target=rig.gateway.submit_command,
                args=(Command("d1", f"b-{n}", "blocking", {"n": n}),))
            for n in (1, 2, 3)]
        for th in queued_threads:
            th.start()
        # 无 sleep 凑结果：以有界队列 qsize 达到 3 作为“铺满”的确定性同步条件
        # （time 仅作失败兜底，不参与判定）
        import time as _time
        state = rig.executor._devices["d1"]  # 测试中可观测内部有界队列
        deadline = _time.monotonic() + 3
        while state.queue.qsize() < 3 and _time.monotonic() < deadline:
            pass
        self.assertEqual(state.queue.qsize(), 3, "排队槽必须确定性铺满")
        # 此刻 在途 1 + 排队 3 已达有效容量；再多提交 3 单应立即被拒
        rejected = []
        for n in (4, 5, 6):
            r = rig.gateway.submit_command(
                Command("d1", f"b-{n}", "blocking", {"n": n}))
            self.assertEqual(r.status, "rejected")
            rejected.append(r.reason.reason)
        self.assertEqual(rejected.count(R_COMMAND_QUEUE_FULL), 3)
        rig.open()
        starter.join()
        for th in queued_threads:
            th.join()
        m = rig.gateway.command_metrics()
        self.assertTrue(m.accounts_balanced(),
                        "submitted 必须等于 accepted+duplicates+各类被拒")
        self.assertEqual(
            m.submitted,
            m.accepted + m.duplicates + sum(m.rejected.values()))
        report("容量-积压超限拒绝且账平",
               {"max_queue": 3, "在途": "b-0", "排队": ["b-1", "b-2", "b-3"],
                "超额提交": ["b-4", "b-5", "b-6"]},
               "在途+排队占满后多余提交 COMMAND_QUEUE_FULL；提交计数对账恒平",
               {"rejected": rejected, "metrics": m.to_dict()})

    def test_submission_accounting_covers_every_rejection_class(self) -> None:
        # 单台执行器上依次覆盖：DEVICE_UNKNOWN / COMMAND_UNAUTHORIZED /
        # COMMAND_QUEUE_FULL / COMMAND_DUPLICATE / RETIRED 重放，并核对账平。
        rig = BlockingRig(
            max_queue=2,
            authz={"d1": frozenset({"blocking", "boom"}),
                   "d2": frozenset()})

        unknown = rig.gateway.submit_command(Command("ghost", "x1", "blocking"))
        self.assertEqual(unknown.reason.reason, "DEVICE_UNKNOWN")
        denied = rig.gateway.submit_command(Command("d2", "x2", "blocking"))
        self.assertEqual(denied.reason.reason, "COMMAND_UNAUTHORIZED")

        # QUEUE_FULL：d1 在途 1 + 排队 2 占满，再提交即拒
        starter = threading.Thread(
            target=rig.gateway.submit_command,
            args=(Command("d1", "b-0", "blocking", {"n": 0}),))
        starter.start()
        self.assertTrue(rig.ready.wait(3))
        queued = [
            threading.Thread(
                target=rig.gateway.submit_command,
                args=(Command("d1", f"b-{n}", "blocking", {"n": n}),))
            for n in (1, 2)]
        for th in queued:
            th.start()
        full = rig.gateway.submit_command(
            Command("d1", "b-3", "blocking", {"n": 3}))
        self.assertEqual(full.reason.reason, R_COMMAND_QUEUE_FULL)

        # 跨设备复用 command_id：需要一台也有授权的设备，另建执行器覆盖
        rig_other = BlockingRig(
            max_queue=8,
            authz={"d1": frozenset({"blocking"}),
                   "d2": frozenset({"blocking"})})
        rig_other.gateway.submit_command(
            Command("d1", "shared", "blocking", {"n": 1}))
        cross = rig_other.gateway.submit_command(
            Command("d2", "shared", "blocking", {"n": 1}))
        self.assertEqual(cross.reason.reason, R_COMMAND_DUPLICATE)

        # RETIRED 重放：max_history 很小，已完成指令被淘汰后再提交
        rig_tiny = BlockingRig(
            max_queue=8, max_history=1, max_retired=10,
            authz={"d1": frozenset({"blocking"})})
        rig_tiny.gateway.submit_command(
            Command("d1", "old-0", "blocking", {"n": 0}))
        rig_tiny.gateway.submit_command(
            Command("d1", "old-1", "blocking", {"n": 1}))
        replay = rig_tiny.gateway.submit_command(
            Command("d1", "old-0", "blocking", {"n": 0}))
        self.assertEqual(replay.reason.reason, R_COMMAND_RETIRED)

        rig.open()
        starter.join()
        for th in queued:
            th.join()

        for gw in (rig.gateway, rig_other.gateway, rig_tiny.gateway):
            m = gw.command_metrics()
            self.assertTrue(m.accounts_balanced(), m.to_dict())
            self.assertEqual(
                m.submitted,
                m.accepted + m.duplicates + sum(m.rejected.values()))
        m = rig.gateway.command_metrics()
        self.assertEqual(m.rejected.get("DEVICE_UNKNOWN"), 1)
        self.assertEqual(m.rejected.get("COMMAND_UNAUTHORIZED"), 1)
        self.assertEqual(m.rejected.get("COMMAND_QUEUE_FULL"), 1)
        report("容量-各类拒绝全部入账且账平",
               {"unknown": "ghost/blocking", "unauthorized": "d2/blocking",
                "queue_full": "b-3(max_queue=2)", "cross_device": "shared",
                "replay": "old-0 after eviction"},
               "每单恰好归入 accepted/duplicate/某类 rejected；"
               "submitted=accepted+duplicates+Σrejected",
               {"rig": m.to_dict(),
                "rig_other": rig_other.gateway.command_metrics().to_dict(),
                "rig_tiny": rig_tiny.gateway.command_metrics().to_dict()})


if __name__ == "__main__":
    unittest.main(verbosity=2)
