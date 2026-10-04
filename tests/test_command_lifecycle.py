"""指令生命周期新增能力单测：事后查询 / 撤销 / 有界容量与对账。

并发窗口全部由 Event/Barrier 确定性地构造（不靠 sleep 撞运气）：
  * 在途指令用闸门卡死 => 其后的指令确定性处于 PENDING（撤销窗口）；
  * 多线程同时撤销同一条排队指令 => 恰好一次成功、其余幂等；
  * 被撤销的排队指令由工作线程跳过 => 绝不执行、同设备顺序不变。
每个用例打印：输入、判定依据、最终状态。
"""
from __future__ import annotations

import json
import threading
import unittest

from gateway.commands import Command, CommandExecutor
from gateway.gateway import Gateway


def report(title: str, frames, verdict: str, final_state) -> None:
    print("\n" + "=" * 72)
    print(f"用例: {title}")
    print(f"输入报文: {json.dumps(frames, ensure_ascii=False, sort_keys=True)}")
    print(f"判定依据: {verdict}")
    print(f"最终状态: {final_state}")


class BlockingSetup:
    """确定性构造：第 0 条指令在途阻塞；started 表示其已 EXECUTING。"""

    def __init__(self, max_queue: int = 8, max_status_records: int = 1024,
                 authz: frozenset | None = None) -> None:
        self.gate = threading.Event()
        self.started = threading.Event()
        self.executed: list[str] = []
        self.log_lock = threading.Lock()
        self._primed = False

        def blocking(device_id: str, args: dict) -> dict:
            with self.log_lock:
                self.executed.append(args["tag"])
                if not self._primed:
                    # 第一条真正进入执行的指令 => 确定性地占据在途槽位
                    self._primed = True
                    self.started.set()
            self.gate.wait(timeout=5)
            return {"tag": args["tag"]}

        # "unhandled" 已授权但无处理器：用于在执行期产生 COMMAND_UNSUPPORTED
        names = authz if authz is not None else frozenset({"blocking", "unhandled"})
        self.executor = CommandExecutor(
            authorizations={"d1": names},
            handlers={"blocking": blocking},
            max_queue=max_queue,
            max_status_records=max_status_records,
        )
        self.gw = Gateway(self.executor, frozenset({"d1"}))

    def submit_bg(self, tag: str) -> tuple[threading.Thread, dict]:
        out: dict = {}
        t = threading.Thread(
            target=lambda: out.update(r=self.gw.submit_command(
                Command("d1", tag, "blocking", {"tag": tag}))),
            name=f"submit-{tag}",
        )
        t.start()
        return t, out

    def release(self) -> None:
        self.gate.set()


class CommandQueryTests(unittest.TestCase):
    def test_query_success_rejected_duplicate_unknown_are_distinguishable(self) -> None:
        kit = BlockingSetup()
        t0, r0 = kit.submit_bg("c0")
        self.assertTrue(kit.started.wait(timeout=3))

        # c0 在途：查询应稳定显示 executing
        v_exec = kit.gw.query_command("c0")
        self.assertEqual(v_exec.status, "executing")
        self.assertFalse(v_exec.duplicate)

        # 无此号：明确、可区分的答复
        self.assertIsNone(kit.gw.query_command("never-submitted"))

        # 一条执行期会被拒的指令：已授权但无处理器（排队在 c0 之后，放行后执行）
        rj_result: dict = {}
        rj_thread = threading.Thread(
            target=lambda: rj_result.update(
                r=kit.gw.submit_command(Command("d1", "rj2", "unhandled"))))
        rj_thread.start()
        kit.release()
        t0.join(timeout=3)
        rj_thread.join(timeout=3)
        self.assertEqual(rj_result["r"].reason.reason, "COMMAND_UNSUPPORTED")

        self.assertEqual(kit.gw.query_command("c0").status, "succeeded")
        v_rj = kit.gw.query_command("rj2")
        self.assertEqual(v_rj.status, "rejected")
        self.assertEqual(v_rj.reason.reason, "COMMAND_UNSUPPORTED")
        # 反复查询同一号，结论稳定一致
        for _ in range(5):
            self.assertEqual(kit.gw.query_command("rj2").reason.reason,
                             "COMMAND_UNSUPPORTED")
        # 鉴权即拒同样可查
        denied = kit.gw.submit_command(Command("ghost-dev", "g1", "blocking"))
        self.assertEqual(denied.status, "rejected")
        v_g = kit.gw.query_command("g1")
        self.assertEqual(v_g.status, "rejected")
        self.assertEqual(v_g.reason.reason, "DEVICE_UNKNOWN")
        # 成功 vs 执行期拒绝 vs 提交即拒 vs 无此号 四种答复互不混淆
        statuses = {
            "c0": kit.gw.query_command("c0").status,
            "rj2": kit.gw.query_command("rj2").status,
            "g1": kit.gw.query_command("g1").status,
        }
        self.assertEqual(statuses, {"c0": "succeeded", "rj2": "rejected",
                                    "g1": "rejected"})
        self.assertEqual(kit.gw.query_command("g1").reason.reason, "DEVICE_UNKNOWN")
        report("指令状态查询：成功/拒绝/重复/无此号",
               {"c0": "在途后成功", "rj2": "无处理器", "g1": "未知设备",
                "never-submitted": "未提交过"},
               "executing/succeeded/rejected(UNSUPPORTED/DEVICE_UNKNOWN)/None 各自可区分且稳定",
               {"c0": statuses["c0"],
                "rj2": (v_rj.status, v_rj.reason.reason),
                "g1": kit.gw.query_command("g1").reason.reason,
                "never": kit.gw.query_command("never-submitted")})
        kit.executor.shutdown()

    def test_duplicate_submit_is_distinguishable_and_stable(self) -> None:
        # 同号重复提交：duplicate 标记可区分；终态与首次一致
        ex = CommandExecutor(
            authorizations={"d1": frozenset({"fast"})},
            handlers={"fast": lambda d, a: {"v": 1}},
            max_queue=4, max_status_records=16)
        gw = Gateway(ex, frozenset({"d1"}))
        first = gw.submit_command(Command("d1", "dup-1", "fast"))
        second = gw.submit_command(Command("d1", "dup-1", "fast"))
        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        v = gw.query_command("dup-1")
        self.assertEqual(v.status, "succeeded")
        self.assertFalse(v.duplicate, "查询本身不是重复提交，不打 duplicate")
        ex.shutdown()
        report("重复提交可区分且结论稳定",
               [{"command_id": "dup-1"}] * 2,
               "首次 duplicate=False，复用 duplicate=True；query 终态 succeeded 稳定",
               {"first": first.duplicate, "second": second.duplicate,
                "query": v.status})


class CommandCancelTests(unittest.TestCase):
    def test_cancel_queued_command_skips_execution_and_preserves_order(self) -> None:
        kit = BlockingSetup(max_queue=8)
        t0, _ = kit.submit_bg("c0")
        self.assertTrue(kit.started.wait(timeout=3))
        # c1,c2,c3 确定性排在 c0 后面（PENDING）
        t1, r1 = kit.submit_bg("c1")
        t2, r2 = kit.submit_bg("c2")
        t3, r3 = kit.submit_bg("c3")

        # 等三条都入队（队列 put 完成即可见）：用 registered PENDING 查询确认
        self._wait_pending(kit.gw, ["c1", "c2", "c3"])

        cr = kit.gw.cancel_command("c2")
        self.assertTrue(cr.canceled and not cr.idempotent)
        # 重复取消幂等
        cr_again = kit.gw.cancel_command("c2")
        self.assertTrue(cr_again.canceled and cr_again.idempotent)
        # 提交者最终拿到 canceled 终态（查询也一致）
        kit.release()
        for t in (t0, t1, t2, t3):
            t.join(timeout=3)
        self.assertEqual(r2["r"].status, "canceled")
        self.assertEqual(kit.gw.query_command("c2").status, "canceled")

        # 同设备其它指令顺序不被打乱，c2 既未执行也不影响 c1/c3
        self.assertEqual(kit.executed, ["c0", "c1", "c3"],
                         "取消 c2 后生效顺序必须是 c0,c1,c3")
        self.assertEqual(r1["r"].status, "ok")
        self.assertEqual(r3["r"].status, "ok")
        kit.executor.shutdown()
        report("撤销排队指令：跳过执行且保序",
               {"队列": ["c0(在途)", "c1", "c2(撤销)", "c3"]},
               "PENDING->CANCELED 后工作线程 CAS 失败直接跳过；重复撤销幂等",
               {"c2": r2["r"].status, "生效顺序": kit.executed})

    def test_cancel_executing_or_finished_is_refused_with_reason(self) -> None:
        kit = BlockingSetup()
        t0, r0 = kit.submit_bg("c0")
        self.assertTrue(kit.started.wait(timeout=3))
        too_late = kit.gw.cancel_command("c0")
        self.assertFalse(too_late.canceled)
        self.assertEqual(too_late.reason.reason, "COMMAND_CANCEL_TOO_LATE")
        self.assertEqual(kit.gw.query_command("c0").status, "executing",
                         "撤销被拒不得造成状态回退")
        kit.release()
        t0.join(timeout=3)
        finished = kit.gw.cancel_command("c0")
        self.assertFalse(finished.canceled)
        self.assertEqual(finished.reason.reason, "COMMAND_CANCEL_TOO_LATE")
        self.assertEqual(kit.gw.query_command("c0").status, "succeeded",
                         "终态不得从已完成退回")
        ghost = kit.gw.cancel_command("who")
        self.assertEqual(ghost.reason.reason, "COMMAND_NOT_FOUND")
        kit.executor.shutdown()
        report("撤销在执行/已完成/未知指令",
               {"c0": "在执行时取消→放行→完成后再取消", "who": "未知号"},
               "EXECUTING/SUCCEEDED 均 TOO_LATE；未知号 NOT_FOUND；状态不回退",
               {"executing_cancel": too_late.reason.reason,
                "finished_cancel": finished.reason.reason,
                "ghost": ghost.reason.reason})

    def test_concurrent_cancel_exactly_one_wins_never_executed(self) -> None:
        # 确定性并发窗口：c1 排在在途 c0 之后，确定 PENDING；
        # 8 个线程在屏障处同时撤销 c1，恰好一次真正成功，其余幂等，c1 绝不执行。
        kit = BlockingSetup(max_queue=16)
        t0, _ = kit.submit_bg("c0")
        self.assertTrue(kit.started.wait(timeout=3))
        t1, r1 = kit.submit_bg("c1")
        self._wait_pending(kit.gw, ["c1"])

        results: list = []
        barrier = threading.Barrier(8)
        lock = threading.Lock()

        def fire() -> None:
            barrier.wait()
            out = kit.gw.cancel_command("c1")
            with lock:
                results.append(out)

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3)
        kit.release()
        t0.join(timeout=3)
        t1.join(timeout=3)

        wins = [r for r in results if r.canceled and not r.idempotent]
        idem = [r for r in results if r.idempotent]
        self.assertEqual(len(wins), 1, "并发撤销恰好一次真正改状态")
        self.assertEqual(len(idem), 7, "其余并发撤销全部幂等")
        self.assertNotIn("c1", kit.executed, "被撤销指令绝不能被执行")
        self.assertEqual(r1["r"].status, "canceled")
        kit.executor.shutdown()
        report("并发撤销同一排队指令",
               {"concurrency": 8, "target": "c1(排队中)"},
               "状态机单锁原子：恰好 1 次成功 + 7 次幂等；worker CAS 跳过 c1",
               {"真正成功": len(wins), "幂等": len(idem), "实际执行": kit.executed})

    def test_cancel_race_with_execution_start_is_atomic(self) -> None:
        # 取消与"开始执行"竞争：两结局互斥。压测多轮，不允许出现
        # "既 canceled 又被执行"。
        for round_no in range(20):
            kit = BlockingSetup(max_queue=2)
            t0, _ = kit.submit_bg("c0")
            self.assertTrue(kit.started.wait(timeout=3))
            t1, r1 = kit.submit_bg("c1")  # 排队，PENDING
            self._wait_pending(kit.gw, ["c1"])
            # 取消一定发生在 c1 仍排队时（c0 闸门未放行），成功
            cr = kit.gw.cancel_command("c1")
            self.assertTrue(cr.canceled)
            kit.release()
            t0.join(timeout=3)
            t1.join(timeout=3)
            self.assertNotIn("c1", kit.executed, f"round {round_no}")
            self.assertEqual(r1["r"].status, "canceled")
            kit.executor.shutdown()
        report("取消/开始执行 竞争原子性（20 轮）",
               {"rounds": 20, "target": "排队中的 c1"},
               "c0 在途未放行 => c1 必为 PENDING => 取消必成功且 worker 必跳过",
               {"c1 被执行次数": 0})

    @staticmethod
    def _wait_pending(gw: Gateway, tags: list[str]) -> None:
        """轮询至这些指令都可查到（已入册 PENDING）。状态查询是确定性同步点。"""
        for _ in range(300):
            views = [gw.query_command(t) for t in tags]
            if all(v is not None and v.status == "pending" for v in views):
                return
            threading.Event().wait(0.005)
        raise AssertionError(f"commands never reached pending: {tags}")


class CapacityTests(unittest.TestCase):
    def test_registry_limit_rejects_when_full_of_active_commands(self) -> None:
        # max_status_records=2：让 2 条未结束指令占满（无终态可清理），
        # 第 3 条提交必须被明确拒绝 STATUS_LIMIT，而不是无界增长。
        kit = BlockingSetup(max_queue=8, max_status_records=2)
        t0, _ = kit.submit_bg("a0")
        self.assertTrue(kit.started.wait(timeout=3))
        t1, _ = kit.submit_bg("a1")  # 排队 PENDING
        CommandCancelTests._wait_pending(kit.gw, ["a1"])
        t2, r2 = kit.submit_bg("a2")
        # a2 在登记册阶段即被拒（甚至进不了设备队列），submit 立即返回
        t2.join(timeout=3)
        self.assertEqual(r2["r"].status, "rejected")
        self.assertEqual(r2["r"].reason.reason, "COMMAND_STATUS_LIMIT")
        self.assertEqual(kit.executor._registry.active_count(), 2,
                         "占满容量的是两条未结束指令，绝不能被清理")
        kit.release()
        t0.join(timeout=3)
        t1.join(timeout=3)
        kit.gw.check_command_invariants()
        kit.executor.shutdown()
        report("容量到顶：全是未结束指令时拒绝新提交",
               {"max_status_records": 2, "占用": ["a0 执行中", "a1 排队"],
                "新提交": "a2"},
               "无终态可清理 => 不允许误删未结束指令 => STATUS_LIMIT 明确拒绝",
               {"a2": r2["r"].reason.reason})

    def test_terminal_records_compacted_then_forgotten_but_never_active(self) -> None:
        ex = CommandExecutor(
            authorizations={"d1": frozenset({"fast"})},
            handlers={"fast": lambda d, a: {"v": 1}},
            max_queue=8, max_status_records=2)
        gw = Gateway(ex, frozenset({"d1"}))
        for i in range(6):
            gw.submit_command(Command("d1", f"k{i}", "fast"))
        m = gw.command_metrics()
        self.assertGreaterEqual(m["evicted_compacted"], 1, "终态记录必须被压缩清理")
        self.assertGreaterEqual(m["evicted_forgotten"], 1, "墓碑超窗必须被彻底遗忘")
        # 最近的结论仍在窗口内可查；最早的已明确 NOT_FOUND
        self.assertEqual(gw.query_command("k5").status, "succeeded")
        self.assertIsNone(gw.query_command("k0"))
        # 任何时刻登记册都不超过上限，账平
        self.assertLessEqual(ex._registry.stored_count(), 2)
        gw.check_command_invariants()
        ex.shutdown()
        report("终态压缩→墓碑→遗忘，有界且不碰未结束指令",
               {"max_status_records": 2, "提交": [f"k{i}" for i in range(6)]},
               "只清终态：完整记录压墓碑（结论仍可查/去重），墓碑超窗彻底遗忘",
               {"metrics": m,
                "stored": ex._registry.stored_count(),
                "k5": gw.query_command("k5").status, "k0": gw.query_command("k0")})

    def test_tombstone_window_still_dedupes_same_command_id(self) -> None:
        executed: list[str] = []
        lock = threading.Lock()

        def fast(d, a):
            with lock:
                executed.append(a["cid"])
            return {}

        ex = CommandExecutor(
            authorizations={"d1": frozenset({"fast"})},
            handlers={"fast": fast},
            max_queue=8, max_status_records=1)
        gw = Gateway(ex, frozenset({"d1"}))
        gw.submit_command(Command("d1", "z0", "fast", {"cid": "z0"}))
        gw.submit_command(Command("d1", "z1", "fast", {"cid": "z1"}))  # 挤压 z0
        # 容量为 1：z0 先压成墓碑，z1 再来新的会把 z0 墓碑彻底遗忘；
        # 在墓碑窗口内复用同号必须去重、不重复执行。
        ex2 = CommandExecutor(
            authorizations={"d1": frozenset({"fast"})},
            handlers={"fast": fast},
            max_queue=8, max_status_records=8)
        gw2 = Gateway(ex2, frozenset({"d1"}))
        gw2.submit_command(Command("d1", "w0", "fast", {"cid": "w0"}))
        for _ in range(8):
            gw2.submit_command(Command("d1", "w1", "fast", {"cid": "w1"}))
            gw2.submit_command(Command("d1", "w0", "fast", {"cid": "w0"}))
        self.assertEqual(executed.count("w0"), 1, "窗口内同号只能执行一次")
        self.assertEqual(executed.count("w1"), 1)
        gw2.check_command_invariants()
        ex.shutdown(); ex2.shutdown()
        report("保留窗口内同号不重复执行",
               {"w0 提交": "多次", "w1 提交": "多次"},
               "墓碑窗口仍保留结论：同号复用 duplicate，不二次执行",
               {"w0 执行次数": executed.count("w0"),
                "w1 执行次数": executed.count("w1")})

    def test_submit_ledger_balances_across_mixed_outcomes(self) -> None:
        # 混合：成功、越权、未知设备、跨设备同号、队列满、取消、容量拒绝，
        # 提交总数必须恒等于 受理 + 复用 + 各类受理前拒绝。
        kit = BlockingSetup(max_queue=1, max_status_records=64,
                            authz=frozenset({"blocking"}))
        t0, _ = kit.submit_bg("s0")
        self.assertTrue(kit.started.wait(timeout=3))
        tq1, _ = kit.submit_bg("s1")  # 在途1 + 排队1 = 占满(max_queue=1)
        CommandCancelTests._wait_pending(kit.gw, ["s1"])
        tq2, rfull = kit.submit_bg("s2")  # 队列满，立即拒绝
        tq2.join(timeout=3)
        self.assertEqual(rfull["r"].reason.reason, "COMMAND_QUEUE_FULL")

        unauth = kit.gw.submit_command(Command("d1", "u1", "not-authorized"))
        unknown = kit.gw.submit_command(Command("ghost", "u2", "blocking"))
        cross = kit.gw.submit_command(Command("d2", "s0", "blocking"))
        self.assertEqual(unauth.reason.reason, "COMMAND_UNAUTHORIZED")
        self.assertEqual(unknown.reason.reason, "DEVICE_UNKNOWN")
        self.assertEqual(cross.reason.reason, "COMMAND_DUPLICATE")

        kit.gw.cancel_command("s1")  # 取消排队中的 s1
        kit.release()
        t0.join(timeout=3); tq1.join(timeout=3)
        # 复用同号（重复提交）
        again = kit.gw.submit_command(Command("d1", "s0", "blocking"))
        self.assertTrue(again.duplicate)

        m = kit.gw.command_metrics()
        self.assertEqual(
            m["submitted"],
            m["accepted"] + m["reused_duplicate"]
            + m["rejected_device_unknown"] + m["rejected_unauthorized"]
            + m["rejected_duplicate"] + m["rejected_status_limit"],
            "提交账：总数 == 受理 + 复用 + 各类受理前拒绝",
        )
        self.assertEqual(
            m["accepted"],
            + (m["terminal_succeeded"] + m["terminal_canceled"]
               + m["rejected_queue_full"]),
            "受理账：受理数 == 各终态（本例无未结束、无执行期拒绝）",
        )
        kit.gw.check_command_invariants()
        kit.executor.shutdown()
        report("混合结果下提交账对平",
               {"成功": "s0", "取消": "s1", "队列满": "s2",
                "越权": "u1", "未知设备": "u2", "跨设备同号": "d2/s0", "复用": "s0 再提"},
               "submitted==accepted+reused+pre_rejected；accepted==各终态之和",
               m)


if __name__ == "__main__":
    unittest.main(verbosity=2)
