"""传输层集成测试：真实的 WebSocket 长连接与 HTTP 指令接口。

使用 FastAPI TestClient（其在独立线程中运行 ASGI app），
覆盖：建连续传游标、乱序分片上报、重复 seq 幂等 ACK、非法帧可区分拒绝、
HTTP 授权/越权指令，以及指令结果向在线设备的同步推送。
"""
from __future__ import annotations

import json
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
