"""可选的 FastAPI 传输层：设备长连接（WebSocket）与下行指令（HTTP POST）。

- ``/ws/{device_id}``：设备侧长连接。每个文本帧即一个连接帧（JSON）；
  网关对每一帧回一个 JSON 结果，拒绝时携带可区分的 ``code``。
- ``POST /devices/{device_id}/commands``：平台侧向设备下发指令的入口，
  同步等待该设备串行执行完成（或被鉴权/积压拒绝）。

协议字段约定见 ``docs/protocol.md``，鉴权与并发语义见 ``docs/commands.md``。
"""

from __future__ import annotations

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from .core import Gateway
from .errors import GatewayError
from .protocol import CommandRequest


class CommandIn(BaseModel):
    command: str
    request_id: str
    args: dict = {}


def create_app(gateway: Gateway) -> FastAPI:
    """创建 FastAPI 应用。"""
    app = FastAPI(title="device-telemetry-gateway")

    @app.get("/")
    def read_root() -> dict:
        return {"service": "device-telemetry-gateway", "status": "ok"}

    @app.post("/devices/{device_id}/commands")
    async def submit_command(device_id: str, body: CommandIn) -> dict:
        req = CommandRequest(
            device_id=device_id,
            command=body.command,
            request_id=body.request_id,
            args=body.args,
        )
        result = await gateway.send_command(req)
        return result.to_dict()

    @app.websocket("/ws/{device_id}")
    async def device_ws(websocket: WebSocket, device_id: str) -> None:
        await websocket.accept()
        try:
            while True:
                raw = await websocket.receive_text()
                try:
                    result = gateway.handle_frame(raw)
                except GatewayError as exc:  # 防御：核心层应已自行转为拒绝结果
                    await websocket.send_json(
                        {"accepted": False, "code": exc.code.value, "reason": str(exc)}
                    )
                else:
                    await websocket.send_json(result.to_dict())
        except WebSocketDisconnect:
            gateway.on_disconnect(device_id)

    return app
