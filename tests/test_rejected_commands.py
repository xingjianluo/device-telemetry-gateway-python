"""提交即拒指令的可查询性 / 一致性 / 容量与对账单测（本轮新增）。

覆盖运维场景：
  * 设备不存在 / 未授权 / 积压超限三类“提交那一刻即被拒”的指令，事后凭指令号
    能查到明确的 REJECTED 终态，且三类原因彼此可区分；
  * “被拒”与“从没提交过”是两种明确、可区分的答复（REJECTED 快照 vs
    COMMAND_NOT_FOUND），同一被拒号反复查 / 重复提交 / 并发提交结论稳定一致；
  * 被拒记录不会被误当成执行过/成功，且长期累积同样受 max_history/max_retired
    有界治理；提交对账口径不变（submitted=accepted+duplicates+Σrejected）。

并发窗口全部用确定性闸门（Event）构造，不依赖 sleep 凑结果；time 仅作失败
兜底超时，不参与结果判定。每个用例打印：输入、判定依据、最终状态。
"""
from __future__ import annotations

import json
import threading
import unittest

from gateway.commands import Command, CommandExecutor
from gateway.commandstore import CommandState, CommandStatus
from gateway.gateway import Gateway
from gateway.protocol import (
    R_COMMAND_DUPLICATE,
    R_COMMAND_NOT_CANCELLABLE,
    R_COMMAND_NOT_FOUND,
    R_COMMAND_QUEUE_FULL,
    R_COMMAND_RETIRED,
    R_COMMAND_UNAUTHORIZED,
    R_DEVICE_UNKNOWN,
)


def report(title: str, inp: object, verdict: str, final: object) -> None:
    print("\n" + "=" * 72)
    print(f"用例: {title}")
    print(f"输入: {json.dumps(inp, ensure_ascii=False, sort_keys=True, default=str)}")
    print(f"判定依据: {verdict}")
    print(f"最终状态: {final}")


class Rig:
    """确定性装置：block 处理器 n=0 进入执行即置 ready 并阻塞在 gate。"""

    def __init__(self, max_queue: int = 8, max_history: int = 256,
                 max_retired: int = 4096,
                 authz: dict | None = None) -> None:
        self.gate = threading.Event()
        self.ready = threading.Event()
        self.executed: list[str] = []
        self.log_lock = threading.Lock()

        def block(device_id: str, args: dict) -> dict:
            n = args["n"]
            if n == 0:
                self.ready.set()
                self.gate.wait(timeout=3)
            with self.log_lock:
                self.executed.append(f"{device_id}:{n}")
            return {"n": n}

        self.authz = authz or {"d1": frozenset({"block"})}
        self.executor = CommandExecutor(
            authorizations=self.authz,
            handlers={"block": block},
            max_queue=max_queue, max_history=max_history, max_retired=max_retired,
        )
        self.gateway = Gateway(
            executor=self.executor,
            known_devices=frozenset(self.authz.keys()),
        )

    def open(self) -> None:
        self.gate.set()

    def fill_backlog(self, queue_size: int) -> tuple[threading.Thread, list[threading.Thread]]:
        """票据 0 确定 RUNNING，票据 1..queue_size 确定铺满排队槽。"""
        starter = threading.Thread(
            target=self.gateway.submit_command,
            args=(Command("d1", "b-0", "block", {"n": 0}),))
        starter.start()
        if not self.ready.wait(3):
            raise AssertionError("票据 0 必须确定地进入 RUNNING")
        waiters = [
            threading.Thread(
                target=self.gateway.submit_command,
                args=(Command("d1", f"b-{n}", "block", {"n": n}),))
            for n in range(1, queue_size + 1)
        ]
        for th in waiters:
            th.start()
        q = self.executor._devices["d1"].queue
        while q.qsize() < queue_size:
            pass  # 确定性同步：以有界队列铺满为条件，无 sleep 参与判定
        return starter, waiters


class RejectedQueryTests(unittest.TestCase):
    def test_three_reject_classes_are_recorded_and_distinguishable(self) -> None:
        rig = Rig(max_queue=2)
        starter = None
        waiters: list[threading.Thread] = []
        try:
            # 设备不存在
            r_unknown = rig.gateway.submit_command(
                Command("ghost", "rej-u", "block", {"n": 1}))
            # 未授权（d2 存在但无任何授权指令）
            rig.executor._auth["d2"] = frozenset()
            rig.gateway._known.add("d2")
            r_denied = rig.gateway.submit_command(
                Command("d2", "rej-a", "block", {"n": 1}))
            # 积压超限：在途 1 + 排队 2 占满 max_queue=2
            starter, waiters = rig.fill_backlog(2)
            r_full = rig.gateway.submit_command(
                Command("d1", "rej-f", "block", {"n": 9}))
        finally:
            rig.open()
            if starter is not None:
                starter.join()
                for th in waiters:
                    th.join()

        self.assertEqual(r_unknown.reason.reason, R_DEVICE_UNKNOWN)
        self.assertEqual(r_denied.reason.reason, R_COMMAND_UNAUTHORIZED)
        self.assertEqual(r_full.reason.reason, R_COMMAND_QUEUE_FULL)

        # 事后凭号查询：全部是明确的 REJECTED 终态，原因彼此可区分
        q_unknown = rig.gateway.query_command("rej-u")
        q_denied = rig.gateway.query_command("rej-a")
        q_full = rig.gateway.query_command("rej-f")
        for q in (q_unknown, q_denied, q_full):
            self.assertEqual(q.state, CommandState.REJECTED)
        self.assertEqual(q_unknown.reason.reason, R_DEVICE_UNKNOWN)
        self.assertEqual(q_denied.reason.reason, R_COMMAND_UNAUTHORIZED)
        self.assertEqual(q_full.reason.reason, R_COMMAND_QUEUE_FULL)
        # 被拒指令绝不执行
        self.assertNotIn("d1:9", rig.executed)
        # 积压消散后结论不“翻案”：查询仍 REJECTED，同号重提为 duplicate 被拒，
        # 不会因为现在有空位就把该号补执行
        re_after_drain = rig.gateway.submit_command(
            Command("d1", "rej-f", "block", {"n": 9}))
        self.assertTrue(re_after_drain.duplicate)
        self.assertEqual(re_after_drain.status, "rejected")
        self.assertEqual(re_after_drain.reason.reason, R_COMMAND_QUEUE_FULL)
        self.assertNotIn("d1:9", rig.executed)
        self.assertEqual(
            rig.gateway.query_command("rej-f").state, CommandState.REJECTED)
        report("被拒查询-三类原因可区分",
               {"unknown": "ghost/rej-u", "unauthorized": "d2/rej-a",
                "queue_full": "d1/rej-f (max_queue=2 铺满)"},
               "三类提交即拒均登记 REJECTED 终态，携带各自原因码，且从不执行",
               {"rej-u": q_unknown.reason.reason,
                "rej-a": q_denied.reason.reason,
                "rej-f": q_full.reason.reason,
                "executed": rig.executed})

    def test_rejected_query_is_stable_on_repeated_reads(self) -> None:
        rig = Rig()
        rig.gateway.submit_command(Command("ghost", "same-id", "block", {"n": 1}))
        snapshots = [rig.gateway.query_command("same-id") for _ in range(5)]
        self.assertTrue(all(s.state is CommandState.REJECTED for s in snapshots))
        self.assertTrue(all(
            s.reason.reason == R_DEVICE_UNKNOWN for s in snapshots))
        self.assertTrue(all(s.device_id == "ghost" for s in snapshots))
        report("被拒查询-反复查结论稳定",
               {"submit": "ghost/same-id", "query_times": 5},
               "同一被拒号连续查询返回一致的 REJECTED 快照（状态/原因/设备）",
               {"state": snapshots[0].state.value,
                "reason": snapshots[0].reason.reason,
                "all_equal": len({(s.state, s.reason.reason, s.device_id)
                                  for s in snapshots}) == 1})

    def test_rejected_is_distinct_from_never_submitted(self) -> None:
        rig = Rig()
        rig.gateway.submit_command(Command("ghost", "was-rejected", "block"))
        rejected_status = rig.gateway.query_command("was-rejected")
        never = rig.gateway.query_command("never-ever")
        # 一个是 REJECTED 终态快照，一个是 ParseError(NOT_FOUND)，类型即可区分
        self.assertEqual(rejected_status.state, CommandState.REJECTED)
        self.assertEqual(never.reason, R_COMMAND_NOT_FOUND)
        self.assertNotEqual(type(rejected_status), type(never))
        report("被拒查询-与从没提交明确区分",
               {"rejected": "was-rejected", "never_submitted": "never-ever"},
               "被拒 -> REJECTED 快照(200)；从未提交 -> COMMAND_NOT_FOUND(404)",
               {"was-rejected": rejected_status.state.value,
                "never-ever": never.reason,
                "types_distinct": type(rejected_status).__name__
                                  != type(never).__name__})


class RejectedResubmitTests(unittest.TestCase):
    def test_resubmit_same_rejected_id_is_duplicate_with_same_verdict(self) -> None:
        rig = Rig()
        first = rig.gateway.submit_command(Command("ghost", "dup-rej", "block"))
        second = rig.gateway.submit_command(Command("ghost", "dup-rej", "block"))
        third = rig.gateway.submit_command(Command("ghost", "dup-rej", "block"))
        self.assertEqual(first.status, "rejected")
        self.assertFalse(first.duplicate)
        # 再次提交：结论一致（同样被拒、同样原因），但归类为 duplicate 不重复计拒
        self.assertTrue(second.duplicate and third.duplicate)
        self.assertEqual(second.reason.reason, R_DEVICE_UNKNOWN)
        self.assertEqual(third.reason.reason, R_DEVICE_UNKNOWN)
        q = rig.gateway.query_command("dup-rej")
        self.assertEqual(q.state, CommandState.REJECTED)
        # 绝不被误当成执行过/成功；处理器从未触达
        self.assertEqual(rig.executed, [])
        m = rig.gateway.command_metrics()
        self.assertEqual(m.submitted, 3)
        self.assertEqual(m.duplicates, 2)
        self.assertEqual(m.rejected.get(R_DEVICE_UNKNOWN), 1)
        self.assertEqual(m.accepted, 0)
        self.assertTrue(m.accounts_balanced())
        report("被拒重提-同号复用稳定结论且账平",
               {"submissions": [("ghost", "dup-rej")] * 3},
               "首次被拒登记；后续同设备同号 duplicate 复用 REJECTED，拒绝只计一次",
               m.to_dict())

    def test_concurrent_submissions_of_rejected_id_have_single_verdict(self) -> None:
        rig = Rig()
        barrier = threading.Barrier(8)
        results: list = []
        lock = threading.Lock()

        def fire(i: int) -> None:
            barrier.wait()  # 8 个提交精确同时竞争同一未注册设备上的同一号
            r = rig.gateway.submit_command(
                Command("ghost", "race-rej", "block", {"n": i}))
            with lock:
                results.append(r)

        threads = [threading.Thread(target=fire, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 恰好一个“首单”，其余 7 个 duplicate；没有任何一个执行成功
        firsts = [r for r in results if not r.duplicate]
        dups = [r for r in results if r.duplicate]
        self.assertEqual(len(firsts), 1)
        self.assertEqual(len(dups), 7)
        self.assertTrue(all(r.status == "rejected" for r in results))
        reasons = {r.reason.reason for r in results}
        self.assertEqual(reasons, {R_DEVICE_UNKNOWN})
        q = rig.gateway.query_command("race-rej")
        self.assertEqual(q.state, CommandState.REJECTED)
        self.assertEqual(rig.executed, [])
        m = rig.gateway.command_metrics()
        self.assertEqual(m.submitted, 8)
        self.assertEqual(m.duplicates, 7)
        self.assertEqual(m.rejected.get(R_DEVICE_UNKNOWN), 1)
        self.assertTrue(m.accounts_balanced())
        report("被拒并发-同号竞争单一结论",
               {"concurrency": 8, "command_id": "race-rej", "device": "ghost"},
               "锁内串行归类：1 次被拒 + 7 次 duplicate，全员结论一致且从不执行",
               {"firsts": len(firsts), "duplicates": len(dups),
                "query": q.state.value, "metrics": m.to_dict()})

    def test_resubmit_rejected_id_from_another_device_is_duplicate_reject(self) -> None:
        rig = Rig(authz={"d1": frozenset({"block"}),
                        "d2": frozenset({"block"})})
        # 首单以未授权被拒（设备 d3 不存在于授权表）
        first = rig.gateway.submit_command(Command("d3", "cross-rej", "block"))
        self.assertEqual(first.reason.reason, R_DEVICE_UNKNOWN)
        # 换一台已授权设备复用同一 command_id：沿用 COMMAND_DUPLICATE 规则，
        # 绝不允许借“换设备重提”让同一指令号被执行
        again = rig.gateway.submit_command(Command("d1", "cross-rej", "block"))
        self.assertEqual(again.status, "rejected")
        self.assertEqual(again.reason.reason, R_COMMAND_DUPLICATE)
        q = rig.gateway.query_command("cross-rej")
        self.assertEqual(q.state, CommandState.REJECTED)
        self.assertEqual(rig.executed, [])
        report("被拒重提-跨设备复用仍拒绝",
               {"first": "d3/cross-rej(设备不存在)", "again": "d1/cross-rej"},
               "窗内 command_id 已归属 d3 -> COMMAND_DUPLICATE，同号不可能被执行",
               {"again": again.reason.reason, "owner": q.device_id,
                "query_state": q.state.value})


class RejectedCapacityTests(unittest.TestCase):
    def test_rejected_records_are_bounded_by_history_and_tombstones(self) -> None:
        rig = Rig(max_history=3, max_retired=5)
        for i in range(12):
            rig.gateway.submit_command(
                Command("ghost", f"rej-{i:02d}", "block"))
        m = rig.gateway.command_metrics()
        # 被拒记录同样是终态：保留窗 <= max_history，墓碑 <= max_retired，有界
        self.assertLessEqual(m.history, 3)
        self.assertLessEqual(m.retired, 5)
        self.assertEqual(m.active, 0, "被拒记录从建立起就是终态，不占活跃容量")
        self.assertEqual(m.submitted, 12)
        self.assertEqual(m.accepted, 0)
        self.assertEqual(m.rejected.get(R_DEVICE_UNKNOWN), 12)
        self.assertTrue(m.accounts_balanced())
        # 保留窗/墓碑窗内：仍可查到 REJECTED，且重提交走 RETIRED/duplicate 规则
        in_window = rig.gateway.query_command("rej-11")
        self.assertEqual(in_window.state, CommandState.REJECTED)
        # 最老的超出墓碑窗 -> 彻底遗忘，回到 NOT_FOUND（与“被拒”仍明确区分）
        forgotten = rig.gateway.query_command("rej-00")
        self.assertEqual(forgotten.reason, R_COMMAND_NOT_FOUND)
        report("被拒容量-保留窗与墓碑环有界",
               {"rejected_submissions": 12, "max_history": 3, "max_retired": 5},
               "被拒终态同样 FIFO 进保留窗、再进墓碑环，总量严格有界；超窗回 NOT_FOUND",
               {"metrics": m.to_dict(),
                "rej-11": in_window.state.value,
                "rej-00": forgotten.reason})

    def test_rejected_id_resubmit_after_retirement_is_retired_not_executed(self) -> None:
        # 被拒记录被容量治理淘汰为墓碑后，同号重提 -> COMMAND_RETIRED，仍不执行
        rig = Rig(max_history=1, max_retired=2)
        rig.gateway.submit_command(Command("ghost", "old-rej", "block"))
        for i in range(2):  # max_history=1 + max_retired=2：old-rej 确定落在墓碑窗
            rig.gateway.submit_command(
                Command("ghost", f"push-{i}", "block"))
        q = rig.gateway.query_command("old-rej")
        if isinstance(q, CommandStatus):
            # 仍在墓碑窗：RETIRED 重放
            self.assertTrue(q.retired)
            self.assertEqual(q.state, CommandState.REJECTED)
            again = rig.gateway.submit_command(Command("ghost", "old-rej", "block"))
            self.assertEqual(again.reason.reason, R_COMMAND_RETIRED)
            verdict = "tombstone -> COMMAND_RETIRED"
        else:  # 已彻底遗忘：等同新号，按设备不存在再次被拒，仍可查
            again = rig.gateway.submit_command(Command("ghost", "old-rej", "block"))
            self.assertEqual(again.reason.reason, R_DEVICE_UNKNOWN)
            verdict = "forgotten -> fresh DEVICE_UNKNOWN rejection"
        self.assertEqual(rig.executed, [])
        m = rig.gateway.command_metrics()
        self.assertTrue(m.accounts_balanced())
        report("被拒容量-淘汰后重提不执行",
               {"first": "ghost/old-rej", "push": 2, "max_history": 1,
                "max_retired": 2},
               f"被拒记录同样受容量治理；{verdict}；任何路径都不会执行",
               {"verdict": verdict, "metrics": m.to_dict()})

    def test_mixed_accepted_and_rejected_share_one_bounded_pipeline(self) -> None:
        """被拒与已完成终态共用同一条 FIFO：互相挤占但总量恒为 max_history。"""
        rig = Rig(max_history=4, max_retired=8)
        # 交替制造：被拒(ghost) + 成功(d1)
        for i in range(10):
            rig.gateway.submit_command(
                Command("ghost", f"r-{i}", "block"))
            self.assertEqual(
                rig.gateway.submit_command(
                    Command("d1", f"ok-{i}", "block", {"n": i + 100})).status,
                "ok")
        m = rig.gateway.command_metrics()
        self.assertLessEqual(m.history, 4)
        self.assertLessEqual(m.retired, 8)
        self.assertEqual(m.submitted, 20)
        self.assertEqual(m.accepted, 10)
        self.assertEqual(m.rejected.get(R_DEVICE_UNKNOWN), 10)
        self.assertEqual(m.terminal.get("succeeded"), 10)
        self.assertEqual(m.terminal.get("rejected"), 10)
        self.assertTrue(m.accounts_balanced())
        # 被推进墓碑的成功指令仍是 succeeded（淘汰前终态不被被拒记录污染）
        report("被拒容量-与成功终态共用有界 FIFO",
               {"pattern": ["reject", "ok"] * 10, "max_history": 4,
                "max_retired": 8},
               "两类终态统一进 FIFO 治理；账目分类不变，终态原因互不污染",
               m.to_dict())


class RejectedCancelAndAccountingTests(unittest.TestCase):
    def test_cancel_rejected_id_is_not_cancellable_but_verdict_kept(self) -> None:
        rig = Rig()
        rig.gateway.submit_command(Command("ghost", "cant-cancel", "block"))
        c = rig.gateway.cancel_command("cant-cancel")
        self.assertEqual(c.status, "rejected")
        self.assertEqual(c.reason.reason, R_COMMAND_NOT_CANCELLABLE)
        # 取消被拒不改变原结论：仍是 REJECTED/DEVICE_UNKNOWN，绝不变成已取消
        q = rig.gateway.query_command("cant-cancel")
        self.assertEqual(q.state, CommandState.REJECTED)
        self.assertEqual(q.reason.reason, R_DEVICE_UNKNOWN)
        report("被拒取消-不可取消但原结论保留",
               {"submit": "ghost/cant-cancel", "cancel": "cant-cancel"},
               "REJECTED 属终态 -> COMMAND_NOT_CANCELLABLE；取消不改写为 cancelled",
               {"cancel": c.reason.reason, "state": q.state.value,
                "reason": q.reason.reason})

    def test_rejected_submissions_keep_global_accounts_balanced(self) -> None:
        rig = Rig(max_queue=1)
        # 设备不存在 x2、未授权 x1
        rig.gateway.submit_command(Command("ghost", "g1", "block"))
        rig.gateway.submit_command(Command("ghost", "g2", "block"))
        rig.executor._auth["d2"] = frozenset()
        rig.gateway._known.add("d2")
        rig.gateway.submit_command(Command("d2", "a1", "block"))
        # 积压超限 x2（在途 1 + 排队 1 铺满 max_queue=1）
        starter, waiters = rig.fill_backlog(1)
        try:
            f1 = rig.gateway.submit_command(Command("d1", "f1", "block"))
            f2 = rig.gateway.submit_command(Command("d1", "f2", "block"))
        finally:
            rig.open()
            starter.join()
            for th in waiters:
                th.join()
        self.assertEqual(f1.reason.reason, R_COMMAND_QUEUE_FULL)
        self.assertEqual(f2.reason.reason, R_COMMAND_QUEUE_FULL)
        # 被拒号重提 -> duplicate，不新增拒绝计数
        rig.gateway.submit_command(Command("ghost", "g1", "block"))
        m = rig.gateway.command_metrics()
        self.assertEqual(m.submitted, 8)           # 6 拒 + 2 接受
        self.assertEqual(m.accepted, 2)
        self.assertEqual(m.duplicates, 1)
        self.assertEqual(m.rejected.get(R_DEVICE_UNKNOWN), 2)
        self.assertEqual(m.rejected.get(R_COMMAND_UNAUTHORIZED), 1)
        self.assertEqual(m.rejected.get(R_COMMAND_QUEUE_FULL), 2)
        self.assertEqual(
            m.submitted,
            m.accepted + m.duplicates + sum(m.rejected.values()))
        self.assertTrue(m.accounts_balanced())
        report("被拒对账-口径保持不变",
               {"unknown": 2, "unauthorized": 1, "queue_full": 2,
                "accepted": 2, "duplicate_resubmit": 1},
               "submitted = accepted + duplicates + Σrejected 恒成立",
               m.to_dict())


if __name__ == "__main__":
    unittest.main(verbosity=2)
