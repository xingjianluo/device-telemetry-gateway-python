"""设备接入网关单测。

覆盖场景：
  1) 分片乱序重组 / 重复分片(幂等与冲突) / 残缺拒绝
  2) 同一序列号重复上报不得产生重复业务记录
  3) 断开重连续传：已确认数据不丢失、不重复；未确认报文重传
  4) 协议版本兼容：可选忽略、必填缺失拒绝、未知必填字段拒绝、缺省可复现
  5) 下行指令：越权拒绝(原因可区分)、串行生效与提交顺序、并发幂等、积压超限拒绝
每个用例都会打印：输入报文、判定依据、最终状态。
"""
from __future__ import annotations

import json
import threading
import time
import unittest

from gateway.commands import Command, CommandExecutor
from gateway.gateway import Gateway
from gateway.protocol import parse_fragment_header, parse_frame
from gateway.telemetry import normalize_telemetry


def build_gateway(max_queue: int = 16) -> Gateway:
    """构造带真实指令处理器的网关。

    reboot 会记录执行轨迹并短暂休眠，用于验证串行顺序与并发不重复执行。
    """
    execution_log: list[tuple[str, str]] = []
    log_lock = threading.Lock()

    def reboot(device_id: str, args: dict) -> dict:
        with log_lock:
            execution_log.append((device_id, "reboot"))
        time.sleep(0.01)
        return {"rebooted": True}

    def set_interval(device_id: str, args: dict) -> dict:
        with log_lock:
            execution_log.append((device_id, f"interval={args.get('seconds')}"))
        return {"interval_seconds": args["seconds"]}

    executor = CommandExecutor(
        authorizations={
            "d1": frozenset({"reboot", "set_interval"}),
            "d2": frozenset({"set_interval"}),
        },
        handlers={"reboot": reboot, "set_interval": set_interval},
        max_queue=max_queue,
    )
    gw = Gateway(executor=executor, known_devices=frozenset({"d1", "d2"}))
    gw.execution_log = execution_log  # type: ignore[attr-defined]
    return gw


def report(title: str, frames, verdict: str, final_state) -> None:
    print("\n" + "=" * 72)
    print(f"用例: {title}")
    print(f"输入报文: {json.dumps(frames, ensure_ascii=False, sort_keys=True)}")
    print(f"判定依据: {verdict}")
    print(f"最终状态: {final_state}")


class ReassemblyTests(unittest.TestCase):
    def test_out_of_order_fragments_produce_one_record(self) -> None:
        gw = build_gateway()
        payload = json.dumps({"temperature": 31.2, "location": "SH"})
        cut = 7
        frames = [
            {"seq": 10, "index": 1, "total": 2, "version": "1", "chunk": payload[cut:]},
            {"seq": 10, "index": 0, "total": 2, "version": "1", "chunk": payload[:cut]},
        ]
        r1 = gw.handle_fragment("d1", **frames[0])
        r2 = gw.handle_fragment("d1", **frames[1])
        records = gw.stored_records("d1")
        self.assertTrue(r1.accepted and not r1.complete)
        self.assertTrue(r2.accepted and r2.complete)
        self.assertEqual(len(records), 1, "乱序分片只能产生一条业务记录")
        report("分片乱序重组", frames,
               "index=1 先到仅缓冲；index=0 到齐后按序拼接并落库",
               f"records={[r.raw for r in records]}")

    def test_duplicate_fragment_idempotent_but_conflict_rejected(self) -> None:
        gw = build_gateway()
        gw.handle_fragment("d1", 20, 0, 2, "1", "abc")
        dup = gw.handle_fragment("d1", 20, 0, 2, "1", "abc")
        conflict = gw.handle_fragment("d1", 20, 0, 2, "1", "XYZ")
        self.assertIsNone(dup.error, "完全相同的重复分片必须幂等")
        self.assertEqual(conflict.error.reason, "DUPLICATE_FRAGMENT_CONFLICT")
        report("重复分片：幂等 vs 冲突",
               [{"seq": 20, "index": 0, "chunk": "abc"},
                {"seq": 20, "index": 0, "chunk": "abc"},
                {"seq": 20, "index": 0, "chunk": "XYZ"}],
               "相同内容重传幂等忽略；同 index 不同内容判 DUPLICATE_FRAGMENT_CONFLICT",
               f"conflict={conflict.error.to_dict()}, 已落库数={len(gw.stored_records('d1'))}")

    def test_incomplete_message_rejected_as_gap(self) -> None:
        gw = build_gateway()
        gw.handle_fragment("d1", 30, 0, 3, "1", "x")
        gw.handle_fragment("d1", 30, 2, 3, "1", "z")  # 缺 index=1
        errors = gw.abort_pending("d1")  # 模拟连接中断
        self.assertTrue(any(e.reason == "FRAGMENT_GAP" for e in errors))
        self.assertEqual(gw.stored_records("d1"), [])
        report("残缺报文拒绝",
               [{"seq": 30, "index": 0, "total": 3}, {"seq": 30, "index": 2, "total": 3}],
               "缺少 index=1 永不自行补全；连接中止按 FRAGMENT_GAP 明确拒绝",
               [e.to_dict() for e in errors])


class DuplicateSeqTests(unittest.TestCase):
    def test_same_seq_retransmitted_after_ack_no_duplicate_record(self) -> None:
        gw = build_gateway()
        payload = json.dumps({"temperature": 19.0, "location": "GZ"})
        first = gw.handle_fragment("d1", 100, 0, 1, "1", payload)
        again = gw.handle_fragment("d1", 100, 0, 1, "1", payload)
        third = gw.handle_fragment("d1", 100, 0, 1, "1", payload)
        records = gw.stored_records("d1")
        self.assertTrue(first.complete)
        self.assertTrue(again.duplicate and third.duplicate)
        self.assertEqual(len(records), 1, "同一序列号重传不得产生重复业务记录")
        report("重复序列号去重",
               [{"seq": 100, "payload": payload}] * 3,
               "首次完成即确认；后续同 seq 直接幂等 ACK，命中 confirmed_seqs",
               f"records={len(records)}, 重复标记={[again.duplicate, third.duplicate]}")

    def test_frame_level_errors_are_distinguishable(self) -> None:
        frame_cases = [
            ("{oops", "MALFORMED_JSON"),
            ("[1,2]", "NOT_AN_OBJECT"),
            ('{"type":"weird"}', "UNKNOWN_FRAME_TYPE"),
        ]
        frame_results = [(text, parse_frame(text)) for text, _ in frame_cases]
        for (text, parsed), (_, expect) in zip(frame_results, frame_cases):
            self.assertEqual(parsed.reason, expect, text)
        # 合法帧但分片头缺字段 -> 由头部校验给出 MISSING_FIELD
        header = parse_fragment_header({"type": "telemetry_fragment"})
        self.assertEqual(header.reason, "MISSING_FIELD")
        report("非法报文原因可区分",
               [t for t, _ in frame_cases] + ['{"type":"telemetry_fragment"}(缺头部字段)'],
               "JSON 语法错/顶层非对象/未知帧类型/缺字段 各自独立原因码",
               [(t, p.reason) for t, p in frame_results]
               + [("missing-header-field", header.reason)])


class ResumeTests(unittest.TestCase):
    def test_reconnect_resume_keeps_confirmed_and_retransmits_only_unknown(self) -> None:
        gw = build_gateway()
        p1 = json.dumps({"temperature": 1, "location": "A"})
        p2 = json.dumps({"temperature": 2, "location": "B"})
        gw.handle_fragment("d1", 1, 0, 1, "1", p1)
        # seq=2 只到一半时断线：被判定为残缺
        gw.handle_fragment("d1", 2, 0, 2, "1", p2[:3])
        gw.abort_pending("d1")
        before = gw.resume_state("d1")

        # ---- 模拟重连：网关进程内会话仍在（同一 Gateway 实例） ----
        hello = gw.resume_state("d1")
        self.assertEqual(before["confirmed_seqs"], [1])
        # 设备重传 seq=1（已确认）与 seq=2（未确认）
        dup = gw.handle_fragment("d1", 1, 0, 1, "1", p1)
        done = gw.handle_fragment("d1", 2, 0, 1, "1", p2)
        records = gw.stored_records("d1")
        self.assertTrue(dup.duplicate, "重连后重传已确认数据不得重复落库")
        self.assertTrue(done.complete, "未确认数据重传后必须成功补齐（不丢失）")
        self.assertEqual([r.seq for r in records], [1, 2])
        report("断开重连续传",
               {"断线前": ["seq=1 完整", "seq=2 仅首片"],
                "重连游标": hello,
                "重传": ["seq=1", "seq=2"]},
               "confirmed_seqs=[1]：seq=1 幂等确认；seq=2 重传补齐，无丢失无重复",
               f"落库序列={[r.seq for r in records]}")


class VersionCompatTests(unittest.TestCase):
    def test_v1_and_v2_devices_coexist_with_stable_defaults(self) -> None:
        gw = build_gateway()
        v1_payload = json.dumps({"temperature": 20, "location": "BJ"})  # humidity 缺省
        v2_payload = json.dumps({"temperature": 20, "humidity": 55, "location": "BJ"})
        r1 = gw.handle_fragment("d1", 1, 0, 1, "1", v1_payload)
        r2 = gw.handle_fragment("d2", 2, 0, 1, "2", v2_payload)
        self.assertTrue(r1.accepted and r2.accepted)
        self.assertIsNone(r1.record.humidity, "v1 缺省 humidity 稳定为 null")
        self.assertEqual(r2.record.raw["firmware"], "unknown", "v2 缺省 firmware 稳定为 unknown")
        # 缺省值可复现：归一化两次结果完全一致
        again = normalize_telemetry("d1", 1, "1", {"temperature": 20, "location": "BJ"})
        self.assertEqual(again.raw, r1.record.raw)
        report("v1/v2 同时接入与缺省稳定性",
               {"v1": v1_payload, "v2": v2_payload},
               "v1 humidity 可选缺省 null；v2 humidity 必填、firmware 可选缺省 unknown",
               {"v1": r1.record.raw, "v2": r2.record.raw})

    def test_optional_unknown_fields_ignored_but_required_missing_rejected(self) -> None:
        # 未知可选字段在 v1 被忽略
        ok = normalize_telemetry("d1", 1, "1",
                                 {"temperature": 1, "location": "X", "future_opt": 9})
        self.assertNotIn("future_opt", ok.raw)
        # 必填缺失
        miss = normalize_telemetry("d1", 1, "1", {"temperature": 1})
        self.assertEqual(miss.reason, "MISSING_FIELD")
        miss_v2 = normalize_telemetry("d1", 1, "2",
                                      {"temperature": 1, "location": "X"})
        self.assertEqual(miss_v2.reason, "MISSING_FIELD")
        report("可选忽略 / 必填缺失",
               [{"v1 带未知可选": {"temperature": 1, "location": "X", "future_opt": 9}},
                {"v1 缺 location": {"temperature": 1}},
                {"v2 缺 humidity": {"temperature": 1, "location": "X"}}],
               "未知可选字段不入库；任何版本的必填缺失一律 MISSING_FIELD",
               {"ignored": ok.raw, "v1_missing": miss.reason, "v2_missing": miss_v2.reason})

    def test_unknown_required_field_must_be_explicitly_rejected(self) -> None:
        # 设备自报必填了一个本版本完全未知的字段：不得按旧版本猜测解析
        payload = {"temperature": 1, "location": "X", "requires": ["digital_signature"]}
        err = normalize_telemetry("d1", 1, "1", payload)
        self.assertEqual(err.reason, "UNKNOWN_REQUIRED_FIELD")
        # 同一报文若误按 v2 猜测也一样拒绝（v2 同样不知 digital_signature）
        err2 = normalize_telemetry("d1", 1, "2", payload)
        self.assertEqual(err2.reason, "UNKNOWN_REQUIRED_FIELD")
        report("未知必填字段明确拒绝",
               payload,
               "requires 声明 digital_signature，v1/v2 均无此字段 -> 拒绝而非猜测",
               {"v1": err.to_dict(), "v2": err2.to_dict()})


class CommandAuthzTests(unittest.TestCase):
    def test_unauthorized_command_rejected_with_distinct_reason(self) -> None:
        gw = build_gateway()
        denied = gw.submit_command(Command("d2", "c-deny", "reboot"))
        unknown = gw.submit_command(Command("ghost", "c-x", "reboot"))
        allowed = gw.submit_command(Command("d1", "c-ok", "reboot"))
        self.assertEqual(denied.reason.reason, "COMMAND_UNAUTHORIZED")
        self.assertEqual(unknown.reason.reason, "DEVICE_UNKNOWN")
        self.assertEqual(allowed.status, "ok")
        report("指令鉴权",
               [{"device": "d2", "name": "reboot"},
                {"device": "ghost", "name": "reboot"},
                {"device": "d1", "name": "reboot"}],
               "越权=COMMAND_UNAUTHORIZED；未注册设备=DEVICE_UNKNOWN；授权通过=ok",
               {"denied": denied.reason.to_dict(),
                "unknown": unknown.reason.to_dict(),
                "allowed": allowed.status})


class CommandConcurrencyTests(unittest.TestCase):
    def test_concurrent_same_command_id_executes_once(self) -> None:
        gw = build_gateway()
        results: list = []
        barrier = threading.Barrier(8)

        def fire() -> None:
            barrier.wait()
            results.append(gw.submit_command(Command("d1", "same-id", "reboot")))

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        executed = gw.execution_log.count(("d1", "reboot"))  # type: ignore[attr-defined]
        self.assertEqual(executed, 1, "并发同 command_id 只能真正执行一次")
        self.assertEqual(sum(1 for r in results if not r.duplicate), 1)
        self.assertEqual(sum(1 for r in results if r.duplicate), 7)
        report("并发同号指令幂等",
               {"concurrency": 8, "command_id": "same-id", "name": "reboot"},
               "首个提交入队执行，其余并发提交等待并复用结果，标记 duplicate",
               {"实际执行次数": executed,
                "duplicate 数": sum(1 for r in results if r.duplicate)})

    def test_same_device_commands_serialized_in_submission_order(self) -> None:
        # 确定性构造积压：先让票据 0 在途并阻塞在闸门，再放开其余提交
        gate = threading.Event()
        ready = threading.Event()
        executed_order: list[int] = []

        def blocking_handler(device_id: str, args: dict) -> dict:
            if args["n"] == 0:
                ready.set()  # 通知：票据 0 已占用唯一在途槽位并将阻塞
            gate.wait(timeout=3)
            executed_order.append(args["n"])
            return {"n": args["n"]}

        executor = CommandExecutor(
            authorizations={"d1": frozenset({"blocking"})},
            handlers={"blocking": blocking_handler},
            max_queue=4,
        )
        gw = Gateway(executor=executor, known_devices=frozenset({"d1"}))

        results: dict = {}
        ticket_lock = threading.Lock()
        next_ticket = 0

        def fire(starter: bool, i: int) -> None:
            nonlocal next_ticket
            if starter:
                # 票据 0 由 starter 线程先提交；待其确认在途阻塞后，其余线程才开始
                next_ticket = 1
                results[0] = gw.submit_command(
                    Command("d1", "ord-0", "blocking", {"n": 0}))
                return
            ready.wait(timeout=3)
            with ticket_lock:
                order_no = next_ticket
                next_ticket += 1
            results[order_no] = gw.submit_command(
                Command("d1", f"ord-{order_no}", "blocking", {"n": order_no}))

        starter = threading.Thread(target=fire, args=(True, 0))
        others = [threading.Thread(target=fire, args=(False, i)) for i in range(1, 10)]
        starter.start()
        ready.wait(timeout=3)        # 票据 0 已在途并阻塞在闸门
        for t in others:
            t.start()
        for t in others:
            t.join()                 # 1..4 排队，5..9 立即 QUEUE_FULL 并返回
        gate.set()                   # 放行：0..4 严格按序执行
        starter.join()

        rejected = [r.reason.reason for r in results.values() if r.status == "rejected"]
        oks = [r for r in results.values() if r.status == "ok"]
        self.assertEqual(sorted(rejected), ["COMMAND_QUEUE_FULL"] * 5,
                         "超过 在途1+队列4 的提交必须确定性地收到可区分拒绝")
        self.assertEqual(sorted(results), list(range(10)))
        # 票据 0..4 入队（0 在途、1..4 排队），严格按提交顺序 0,1,2,3,4 串行生效；
        # 票据 5..9 全部被可区分拒绝，无重复执行、无状态回退
        self.assertEqual(executed_order, [0, 1, 2, 3, 4], "必须按提交顺序串行生效")
        # 调用方拿到结果的先后只取决于等待线程调度，无意义；按 command_id 比较集合
        self.assertEqual({r.command_id for r in oks},
                         {f"ord-{n}" for n in range(5)})
        for r in oks:
            idx = int(r.command_id.split("-")[1])
            self.assertEqual(r.response["n"], idx, "响应不得串扰/回退")
        report("同设备串行 + 积压有界",
               {"concurrency": 10, "max_queue": 4,
                "commands": ["blocking(提交票据 n) n=0..9"], "闸门": "先卡死后放行"},
               "票据 0 在途+1..4 排队占满；票据 5..9 立即 COMMAND_QUEUE_FULL；放行后 0..4 顺序执行",
               {"被拒票据": [k for k, v in results.items() if v.status == "rejected"],
                "拒绝数": len(rejected), "执行顺序": executed_order})

    def test_commands_for_different_devices_run_in_parallel(self) -> None:
        gw = build_gateway(max_queue=8)

        def fire(device: str) -> None:
            gw.submit_command(Command(device, f"p-{device}", "reboot"))

        # 让 d1 的 reboot 在其工作线程上先开始（单设备串行执行中）
        t1 = threading.Thread(target=fire, args=("d1",))
        t1.start()
        time.sleep(0.005)
        # d2 无 reboot 授权：其 set_interval 必须能立即被 d2 自己的队列处理，
        # 不排在 d1 的串行队列之后
        t0 = time.perf_counter()
        r2 = gw.submit_command(Command("d2", "p-d2", "set_interval", {"seconds": 30}))
        elapsed_other = time.perf_counter() - t0
        t1.join(timeout=3)
        self.assertFalse(t1.is_alive(), "d1 指令应正常执行完毕")
        self.assertEqual(r2.status, "ok")
        self.assertLess(elapsed_other, 0.01,
                        "其他设备的指令不应排在 d1 的串行队列后等待")
        report("跨设备互不阻塞",
               {"d1": "reboot(慢)", "d2": "set_interval(快)"},
               "每设备独立队列+线程；d2 指令不等待 d1 的在途指令",
               {"d2_elapsed": round(elapsed_other, 4), "d2_result": r2.status})


if __name__ == "__main__":
    unittest.main(verbosity=2)
