"""WebSocket 传输层冒烟：长连接帧 -> 核心网关 -> JSON 回执。"""

from __future__ import annotations

import base64
import json

from fastapi.testclient import TestClient

from gateway.core import Gateway
from gateway.registry import DeviceRegistry
from gateway.transport import create_app


def test_websocket_uplink_duplicate_and_reject() -> None:
    reg = DeviceRegistry()
    reg.register("d1", set())
    gw = Gateway(reg)
    client = TestClient(create_app(gw))

    payload = {"ts": 1, "metrics": {"t": 1}}
    frame = {
        "v": 1, "type": "telemetry", "device": "d1", "session": "s1", "seq": 1,
        "frag": {"i": 0, "n": 1},
        "payload_b64": base64.b64encode(json.dumps(payload).encode()).decode(),
    }
    with client.websocket_connect("/ws/d1") as ws:
        ws.send_json(frame)
        r1 = ws.receive_json()
        assert r1["accepted"] and r1["completed"] and r1["duplicate"] is False

        ws.send_json(frame)  # 重放
        r2 = ws.receive_json()
        assert r2["accepted"] and r2["duplicate"] is True

        ws.send_text("not-a-json-frame")
        r3 = ws.receive_json()
        assert r3["accepted"] is False and r3["code"] == "frame_not_json"

    assert gw.state_snapshot("d1")["record_count"] == 1


def test_http_command_endpoint_authz_distinction() -> None:
    reg = DeviceRegistry()
    reg.register("d1", {"set_level"})
    gw = Gateway(reg, max_pending=4)
    client = TestClient(create_app(gw))

    ok = client.post(
        "/devices/d1/commands",
        json={"command": "set_level", "request_id": "r1", "args": {"level": 2}},
    )
    assert ok.status_code == 200 and ok.json()["accepted"] is True

    denied = client.post(
        "/devices/d1/commands",
        json={"command": "firmware_flash", "request_id": "r2", "args": {}},
    )
    body = denied.json()
    assert body["accepted"] is False and body["code"] == "unauthorized_command"

    unknown = client.post(
        "/devices/ghost/commands",
        json={"command": "set_level", "request_id": "r3", "args": {}},
    )
    assert unknown.json()["code"] == "device_unknown"
