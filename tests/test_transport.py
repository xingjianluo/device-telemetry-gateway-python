"""传输层集成测试：真实的 WebSocket 长连接与 HTTP 指令接口。

使用 FastAPI TestClient（其在独立线程中运行 ASGI app），
覆盖：建连续传游标、乱序分片上报、重复 seq 幂等 ACK、非法帧可区分拒绝、
HTTP 授权/越权指令，以及指令结果向在线设备的同步推送。
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


def build_cancel_client() -> tuple[TestClient, threading.Event, threading.Event]:
    """带确定性闸门的客户端：slow(n=0) 进入执行即置 ready，并阻塞在 gate。"""
    gate = threading.Event()
    ready = threading.Event()

    def slow(device_id: str, args: dict) -> dict:
        if args.get("n") == 0:
            ready.set()
            gate.wait(timeout=3)
        return {"n": args["n"]}

    executor = CommandExecutor(
        authorizations={"d1": frozenset({"slow"})},
        handlers={"slow": slow},
        max_queue=8, max_history=4, max_retired=8,
    )
    gw = Gateway(executor=executor, known_devices=frozenset({"d1"}))
    return TestClient(create_app(gw)), gate, ready


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

    def test_http_query_cancel_and_metrics(self) -> None:
        client, gate, ready = build_cancel_client()
        # 从未提交的指令号查询 -> 404 COMMAND_NOT_FOUND
        missing = client.get("/commands/ghost")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["error"]["reason"], "COMMAND_NOT_FOUND")

        # 票据 0 在途（阻塞），票据 1 排队
        starter = threading.Thread(target=lambda: client.post(
            "/commands",
            json={"device_id": "d1", "command_id": "q-0", "name": "slow",
                  "args": {"n": 0}}))
        starter.start()
        self.assertTrue(ready.wait(3))
        queued = threading.Thread(target=lambda: client.post(
            "/commands",
            json={"device_id": "d1", "command_id": "q-1", "name": "slow",
                  "args": {"n": 1}}))
        queued.start()
        # 确定性等待排队记录出现（time 仅作失败兜底，不参与判定）
        deadline = time.monotonic() + 3
        while client.get("/commands/q-1").json().get("state") != "queued" \
                and time.monotonic() < deadline:
            pass

        q1 = client.get("/commands/q-1").json()
        self.assertEqual(q1["state"], "queued")

        # 取消排队中 -> 200 cancelled；重复取消幂等
        cancelled = client.post("/commands/q-1/cancel")
        self.assertEqual(cancelled.status_code, 200)
        self.assertEqual(cancelled.json()["status"], "cancelled")
        cancel_again = client.post("/commands/q-1/cancel").json()
        self.assertTrue(cancel_again["duplicate"])

        # 取消执行中 -> 409 COMMAND_NOT_CANCELLABLE
        running = client.post("/commands/q-0/cancel")
        self.assertEqual(running.status_code, 409)
        self.assertEqual(running.json()["error"]["reason"],
                         "COMMAND_NOT_CANCELLABLE")
        # 取消不存在 -> 404 COMMAND_NOT_FOUND
        self.assertEqual(client.post("/commands/zzz/cancel").status_code, 404)

        gate.set()
        starter.join()
        queued.join()

        # 放行后：q-0 成功；q-1 维持 cancelled；反复查询结论一致
        self.assertEqual(client.get("/commands/q-0").json()["state"], "succeeded")
        self.assertEqual(client.get("/commands/q-1").json()["state"], "cancelled")
        self.assertEqual(client.get("/commands/q-1").json()["state"], "cancelled")

        metrics = client.get("/metrics/commands").json()
        self.assertTrue(metrics["accounts_balanced"])
        self.assertEqual(
            metrics["submitted"],
            metrics["accepted"] + metrics["duplicates"]
            + sum(metrics["rejected"].values()))

        print("\n用例: HTTP 指令查询/取消/对账")
        print("输入: 查 ghost / q-0 在途 / q-1 排队; 取消 q-1,q-0,zzz; 放行")
        print("判定依据: NOT_FOUND=404; QUEUED 可取消且幂等; RUNNING 409; 账平")
        print(f"最终状态: missing=404, q-1={cancelled.json()['status']}, "
              f"running=409, metrics={metrics}")

    def test_http_rejected_command_is_queryable_and_distinct_from_404(self) -> None:
        client = build_client()
        # 提交即拒：设备未被授权该指令（d2 已注册但授权集为空）
        denied = client.post("/commands", json={
            "device_id": "d2", "command_id": "http-rej-1", "name": "reboot"})
        self.assertEqual(denied.status_code, 403)
        self.assertEqual(denied.json()["error"]["reason"], "COMMAND_UNAUTHORIZED")

        # 设备号不存在的提交即拒
        unknown = client.post("/commands", json={
            "device_id": "nope", "command_id": "http-rej-2", "name": "reboot"})
        self.assertEqual(unknown.json()["error"]["reason"], "DEVICE_UNKNOWN")

        # 事后凭号查询：200 + 明确 rejected 终态与可区分原因，而非 404
        q1 = client.get("/commands/http-rej-1")
        self.assertEqual(q1.status_code, 200)
        body1 = q1.json()
        self.assertEqual(body1["state"], "rejected")
        self.assertEqual(body1["error"]["reason"], "COMMAND_UNAUTHORIZED")
        q2 = client.get("/commands/http-rej-2")
        self.assertEqual(q2.json()["state"], "rejected")
        self.assertEqual(q2.json()["error"]["reason"], "DEVICE_UNKNOWN")
        # 反复查询结论稳定一致
        self.assertEqual(client.get("/commands/http-rej-1").json(), body1)
        # 与“从未提交”严格区分：后者 404 + COMMAND_NOT_FOUND
        missing = client.get("/commands/never-existed")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["error"]["reason"], "COMMAND_NOT_FOUND")
        # 同号再次提交：403 但 duplicate=true，结论不变
        again = client.post("/commands", json={
            "device_id": "d2", "command_id": "http-rej-1", "name": "reboot"})
        self.assertEqual(again.status_code, 403)
        self.assertTrue(again.json()["duplicate"])
        self.assertEqual(again.json()["error"]["reason"], "COMMAND_UNAUTHORIZED")

        metrics = client.get("/metrics/commands").json()
        self.assertTrue(metrics["accounts_balanced"])
        self.assertEqual(metrics["accepted"], 0, "被拒指令绝不入队执行")
        self.assertEqual(metrics["terminal"]["rejected"], 2)

        print("\n用例: HTTP 提交即拒指令可查询且与 404 区分")
        print("输入: d2 reboot(未授权) / nope reboot(设备不存在) / 同号重提")
        print("判定依据: 提交即拒登记 rejected 终态；查询 200 带原因；从未提交 404")
        print(f"最终状态: http-rej-1={body1}, http-rej-2=rejected, "
              f"missing=404, metrics={metrics}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
