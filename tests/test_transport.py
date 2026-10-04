"""传输层集成测试：真实的 WebSocket 长连接与 HTTP 指令接口。

使用 FastAPI TestClient（其在独立线程中运行 ASGI app），
覆盖：建连续传游标、乱序分片上报、重复 seq 幂等 ACK、非法帧可区分拒绝、
HTTP 授权/越权指令，以及指令结果向在线设备的同步推送；
新增：指令状态查询 GET /commands/{id}、撤销 POST /commands/{id}/cancel、
容量对账 GET /commands-metrics/details。
"""
from __future__ import annotations

import json
import threading
import time
import unittest

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.commands import CommandExecutor
from gateway.gateway import Gateway


def build_client() -> TestClient:
    def reboot(device_id: str, args: dict) -> dict:
        time.sleep(0.01)
        return {"rebooted": True}

    executor = CommandExecutor(
        authorizations={"d1": frozenset({"reboot"}), "d2": frozenset()},
        handlers={"reboot": reboot},
        max_queue=4,
    )
    gw = Gateway(executor=executor, known_devices=frozenset({"d1", "d2"}))
    return TestClient(create_app(gw))


def build_gated_client(max_queue: int = 4, max_status_records: int = 64):
    """返回 (client, state)：state.gate 控制 blocking 指令的在途阻塞窗口。

    started 表示已有一条 blocking 进入执行（确定性同步点，非时间等待）。
    """
    state = type("S", (), {})()
    state.gate = threading.Event()
    state.started = threading.Event()
    state._primed = False
    state.lock = threading.Lock()

    def blocking(device_id: str, args: dict) -> dict:
        with state.lock:
            if not state._primed:
                state._primed = True
                state.started.set()
        state.gate.wait(timeout=5)
        return {"tag": args.get("tag")}

    executor = CommandExecutor(
        authorizations={"d1": frozenset({"blocking"})},
        handlers={"blocking": blocking},
        max_queue=max_queue,
        max_status_records=max_status_records,
    )
    gw = Gateway(executor=executor, known_devices=frozenset({"d1"}))
    return TestClient(create_app(gw)), state, gw


class TransportTests(unittest.TestCase):
    def test_websocket_ingest_and_resume(self) -> None:
        client = build_client()
        with client.websocket_connect("/ws/d1") as ws:
            hello = ws.receive_json()
            self.assertEqual(hello["type"], "resume")
            self.assertEqual(hello["confirmed_seqs"], [])

            payload = json.dumps({"temperature": 12.5, "location": "TJ"})
            cut = 6
            ws.send_json({"type": "telemetry_fragment", "device_id": "d1",
                          "seq": 1, "index": 1, "total": 2, "version": "1",
                          "payload_fragment": payload[cut:]})
            ack1 = ws.receive_json()
            ws.send_json({"type": "telemetry_fragment", "device_id": "d1",
                          "seq": 1, "index": 0, "total": 2, "version": "1",
                          "payload_fragment": payload[:cut]})
            ack2 = ws.receive_json()
            self.assertFalse(ack1["complete"])
            self.assertTrue(ack2["complete"])

            # 重复 seq -> duplicate ACK，无新记录
            ws.send_json({"type": "telemetry_fragment", "device_id": "d1",
                          "seq": 1, "index": 0, "total": 1, "version": "1",
                          "payload_fragment": payload})
            dup = ws.receive_json()
            self.assertTrue(dup["duplicate"])

            # 非法帧 -> 可区分原因
            ws.send_text("{bad json")
            err = ws.receive_json()
            self.assertEqual(err["error"]["reason"], "MALFORMED_JSON")

            # 连接身份与帧内 device_id 不一致 -> 拒绝伪造
            ws.send_json({"type": "telemetry_fragment", "device_id": "someone-else",
                          "seq": 9, "index": 0, "total": 1, "version": "1",
                          "payload_fragment": "{}"})
            mismatch = ws.receive_json()
            self.assertEqual(mismatch["error"]["reason"], "DEVICE_ID_MISMATCH")

        print("\n用例: WS 建连续传/乱序/重复/非法帧")
        print("输入报文: 分片 seq=1 total=2(index=1 先到), 随后整包重传, 以及 '{bad json'")
        print("判定依据: 首帧 resume 给游标；分片到齐落库；重复 seq duplicate；非法帧原因码")
        print(f"最终状态: ack1={ack1['complete']}, ack2={ack2['complete']}, "
              f"dup={dup['duplicate']}, err={err['error']['reason']}, "
              f"mismatch={mismatch['error']['reason']}")

    def test_http_command_authz_and_push(self) -> None:
        client = build_client()
        with client.websocket_connect("/ws/d1") as ws:
            ws.receive_json()  # resume
            ok = client.post("/commands", json={
                "device_id": "d1", "command_id": "http-1", "name": "reboot"}).json()
            pushed = ws.receive_json()  # 指令结果同步推送到设备
            self.assertEqual(ok["status"], "ok")
            self.assertEqual(pushed["type"], "command_result")
            self.assertEqual(pushed["command_id"], "http-1")

            # 越权指令
            denied = client.post("/commands", json={
                "device_id": "d2", "command_id": "http-2", "name": "reboot"})
            self.assertEqual(denied.status_code, 403)
            self.assertEqual(denied.json()["error"]["reason"], "COMMAND_UNAUTHORIZED")

            # 幂等重复
            again = client.post("/commands", json={
                "device_id": "d1", "command_id": "http-1", "name": "reboot"}).json()
            self.assertTrue(again["duplicate"])

        print("\n用例: HTTP 下行指令鉴权/推送/幂等")
        print("输入报文: d1 reboot / d2 reboot(越权) / d1 reboot 同号重发")
        print("判定依据: 授权表校验；结果推送在线设备；同 command_id 复用结果")
        print(f"最终状态: ok={ok['status']}, pushed={pushed['type']}, "
              f"denied=403 COMMAND_UNAUTHORIZED, duplicate={again['duplicate']}")

    def test_http_query_statuses_and_not_found(self) -> None:
        client = build_client()
        client.post("/commands", json={
            "device_id": "d1", "command_id": "q-ok", "name": "reboot"})
        # 成功
        r = client.get("/commands/q-ok")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "succeeded")
        self.assertTrue(r.json()["found"])
        # 被拒（越权）也能查到可区分结论
        client.post("/commands", json={
            "device_id": "d2", "command_id": "q-deny", "name": "reboot"})
        rd = client.get("/commands/q-deny").json()
        self.assertEqual(rd["status"], "rejected")
        self.assertEqual(rd["error"]["reason"], "COMMAND_UNAUTHORIZED")
        # 反复查，结论一致
        self.assertEqual(client.get("/commands/q-ok").json()["status"], "succeeded")
        # 从未提交
        nf = client.get("/commands/no-such")
        self.assertEqual(nf.status_code, 404)
        self.assertEqual(nf.json()["error"]["reason"], "COMMAND_NOT_FOUND")
        self.assertFalse(nf.json()["found"])
        print("\n用例: HTTP 指令查询 成功/被拒/无此号")
        print("判定依据: GET /commands/{id} 返回稳定终态；无此号 404 NOT_FOUND")
        print(f"最终状态: ok={r.json()['status']}, deny={rd['error']['reason']}, "
              f"missing={nf.status_code} {nf.json()['error']['reason']}")

    def test_http_cancel_queued_idempotent_and_too_late(self) -> None:
        client, state, gw = build_gated_client(max_queue=8)
        # c0 占用在途槽位（确定性阻塞在 gate）
        first = threading.Thread(target=lambda: client.post(
            "/commands",
            json={"device_id": "d1", "command_id": "c0",
                  "name": "blocking", "args": {"tag": "c0"}}))
        first.start()
        self.assertTrue(state.started.wait(timeout=3))
        # c1 排队
        queued = threading.Thread(target=lambda: client.post(
            "/commands",
            json={"device_id": "d1", "command_id": "c1",
                  "name": "blocking", "args": {"tag": "c1"}}))
        queued.start()
        self._wait_pending(gw, "c1")

        # 撤销排队中的 c1
        cancel = client.post("/commands/c1/cancel")
        self.assertEqual(cancel.status_code, 200)
        body = cancel.json()
        self.assertTrue(body["canceled"])
        self.assertFalse(body["idempotent"])
        # 重复撤销幂等
        again = client.post("/commands/c1/cancel")
        self.assertTrue(again.json()["idempotent"])
        # 撤销在执行的 c0 -> 409 TOO_LATE
        late = client.post("/commands/c0/cancel")
        self.assertEqual(late.status_code, 409)
        self.assertEqual(late.json()["error"]["reason"], "COMMAND_CANCEL_TOO_LATE")
        # 撤销未知号 -> 404 NOT_FOUND
        nf = client.post("/commands/ghost/cancel")
        self.assertEqual(nf.status_code, 404)
        self.assertEqual(nf.json()["error"]["reason"], "COMMAND_NOT_FOUND")

        state.gate.set()
        first.join(timeout=3)
        queued.join(timeout=3)
        # c0 完成；c1 提交返回 canceled
        self.assertEqual(client.get("/commands/c0").json()["status"], "succeeded")
        self.assertEqual(client.get("/commands/c1").json()["status"], "canceled")
        # 完成后再取消 -> 409
        self.assertEqual(client.post("/commands/c0/cancel").status_code, 409)
        print("\n用例: HTTP 撤销 排队幂等/在途拒绝/未知 404")
        print("判定依据: PENDING 可撤销且幂等；EXECUTING/SUCCEEDED 409；未知 404")
        print(f"最终状态: cancel={body['status']}, again idempotent, "
              f"late=409, ghost=404, c1=canceled, c0=succeeded")

    def test_http_metrics_ledger_and_status_limit(self) -> None:
        client, state, gw = build_gated_client(max_queue=2, max_status_records=2)
        first = threading.Thread(target=lambda: client.post(
            "/commands",
            json={"device_id": "d1", "command_id": "m0",
                  "name": "blocking", "args": {"tag": "m0"}}))
        first.start()
        self.assertTrue(state.started.wait(timeout=3))
        second = threading.Thread(target=lambda: client.post(
            "/commands",
            json={"device_id": "d1", "command_id": "m1",
                  "name": "blocking", "args": {"tag": "m1"}}))
        second.start()
        self._wait_pending(gw, "m1")
        # 容量 2 被两条未结束指令占满 -> 新提交 STATUS_LIMIT
        limited = client.post("/commands", json={
            "device_id": "d1", "command_id": "m2",
            "name": "blocking", "args": {"tag": "m2"}})
        self.assertEqual(limited.status_code, 403)
        self.assertEqual(limited.json()["error"]["reason"], "COMMAND_STATUS_LIMIT")

        metrics = client.get("/commands-metrics/details").json()
        self.assertEqual(
            metrics["submitted"],
            metrics["accepted"] + metrics["reused_duplicate"]
            + metrics["rejected_device_unknown"] + metrics["rejected_unauthorized"]
            + metrics["rejected_duplicate"] + metrics["rejected_status_limit"],
            "HTTP 对账：submitted == accepted + reused + 受理前各类拒绝",
        )
        self.assertGreaterEqual(metrics["rejected_status_limit"], 1)
        gw.check_command_invariants()
        state.gate.set()
        first.join(timeout=3)
        second.join(timeout=3)
        print("\n用例: HTTP 容量上限拒绝与对账")
        print("判定依据: max_status_records=2 且两条未结束 => STATUS_LIMIT；对账计数平衡")
        print(f"最终状态: limit={limited.json()['error']['reason']}, metrics={metrics}")

    @staticmethod
    def _wait_pending(gw: Gateway, command_id: str) -> None:
        for _ in range(300):
            v = gw.query_command(command_id)
            if v is not None and v.status == "pending":
                return
            threading.Event().wait(0.005)
        raise AssertionError(f"{command_id} never reached pending")


if __name__ == "__main__":
    unittest.main(verbosity=2)
