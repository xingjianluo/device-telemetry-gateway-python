"""FastAPI 传输层：WebSocket 长连接 + HTTP 下行指令。"""
from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from gateway.commands import Command
from gateway.commandstore import CommandStatus
from gateway.gateway import Gateway
from gateway.protocol import ParseError, parse_fragment_header, parse_frame


class ConnectionRegistry:
    """device_id -> 已连接 WebSocket，用于把下行指令结果推送到设备。"""

    def __init__(self) -> None:
        self._conns: dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()

    async def add(self, device_id: str, ws: WebSocket) -> None:
        async with self._lock:
            self._conns[device_id] = ws

    async def remove(self, device_id: str, ws: WebSocket) -> None:
        async with self._lock:
            if self._conns.get(device_id) is ws:
                self._conns.pop(device_id, None)

    async def send(self, device_id: str, message: dict[str, Any]) -> bool:
        async with self._lock:
            ws = self._conns.get(device_id)
            if ws is None:
                return False
            await ws.send_json(message)
            return True


def _ingest_to_dict(result: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "type": "telemetry_ack",
        "seq": result.seq,
        "accepted": result.accepted,
        "duplicate": result.duplicate,
        "complete": result.complete,
    }
    if result.error is not None:
        body["error"] = result.error.to_dict()
    if result.record is not None:
        body["record"] = result.record.raw
    return body


def _status_to_dict(status: CommandStatus) -> dict[str, Any]:
    body: dict[str, Any] = {
        "command_id": status.command_id,
        "state": status.state.value,
        "device_id": status.device_id,
        "name": status.name,
        "retired": status.retired,
    }
    if status.response is not None:
        body["response"] = status.response
    if status.reason is not None:
        body["error"] = status.reason.to_dict()
    return body


def create_app(gateway: Gateway) -> FastAPI:
    app = FastAPI(title="device-telemetry-gateway")
    registry = ConnectionRegistry()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/commands")
    async def submit_command(payload: dict[str, Any]) -> JSONResponse:
        required = ("device_id", "command_id", "name")
        missing = [k for k in required if not isinstance(payload.get(k), str)]
        if missing:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "rejected",
                    "error": {"reason": "MISSING_FIELD", "detail": f"need string fields {missing}"},
                },
            )
        args = payload.get("args", {})
        if not isinstance(args, dict):
            return JSONResponse(
                status_code=400,
                content={
                    "status": "rejected",
                    "error": {"reason": "BAD_FIELD_TYPE", "detail": "'args' must be object"},
                },
            )
        command = Command(
            device_id=payload["device_id"],
            command_id=payload["command_id"],
            name=payload["name"],
            args=args,
        )
        # 串行执行发生在工作线程；放到线程池，避免阻塞事件循环
        result = await asyncio.to_thread(gateway.submit_command, command)
        body: dict[str, Any] = {
            "command_id": result.command_id,
            "status": result.status,
            "duplicate": result.duplicate,
        }
        if result.response is not None:
            body["response"] = result.response
        if result.reason is not None:
            body["error"] = result.reason.to_dict()
        # 尽力向在线设备推送指令结果
        await registry.send(
            command.device_id, {"type": "command_result", **body}
        )
        return JSONResponse(status_code=200 if result.status == "ok" else 403, content=body)

    @app.get("/commands/{command_id}")
    async def query_command(command_id: str) -> JSONResponse:
        """凭指令号查询最终/当前状态；查无此号给出明确可区分答复。"""
        status = await asyncio.to_thread(gateway.query_command, command_id)
        if isinstance(status, ParseError):
            return JSONResponse(
                status_code=404, content={"status": "rejected", "error": status.to_dict()}
            )
        return JSONResponse(status_code=200, content=_status_to_dict(status))

    @app.post("/commands/{command_id}/cancel")
    async def cancel_command(command_id: str) -> JSONResponse:
        """取消尚未开始执行的指令；重复取消幂等。"""
        result = await asyncio.to_thread(gateway.cancel_command, command_id)
        body: dict[str, Any] = {
            "command_id": result.command_id,
            "status": result.status,
            "duplicate": result.duplicate,
        }
        if result.response is not None:
            body["response"] = result.response
        if result.reason is not None:
            body["error"] = result.reason.to_dict()
        code = 200 if result.status in ("ok", "cancelled") else (
            404
            if result.reason is not None and result.reason.reason == "COMMAND_NOT_FOUND"
            else 409
        )
        # 取消若已归属某台设备，尽力把取消结论推送给在线设备
        if result.device_id:
            await registry.send(
                result.device_id,
                {"type": "command_result", **body},
            )
        return JSONResponse(status_code=code, content=body)

    @app.get("/metrics/commands")
    async def command_metrics() -> JSONResponse:
        metrics = await asyncio.to_thread(gateway.command_metrics)
        return JSONResponse(status_code=200, content=metrics.to_dict())

    @app.websocket("/ws/{device_id}")
    async def device_ws(ws: WebSocket, device_id: str) -> None:
        await ws.accept()
        gateway.register_device(device_id)
        await registry.add(device_id, ws)
        # 建连即下发续传游标：设备据此只重传未确认数据
        await ws.send_json({"type": "resume", "device_id": device_id,
                           **gateway.resume_state(device_id)})
        try:
            while True:
                text = await ws.receive_text()
                frame = parse_frame(text)
                if isinstance(frame, ParseError):
                    await ws.send_json({"type": "error", "error": frame.to_dict()})
                    continue
                if frame.type.value == "resume":
                    await ws.send_json({"type": "resume", "device_id": device_id,
                                        **gateway.resume_state(device_id)})
                    continue
                if frame.type.value != "telemetry_fragment":
                    await ws.send_json({
                        "type": "error",
                        "error": {"reason": "UNKNOWN_FRAME_TYPE",
                                  "detail": f"devices may not send {frame.type.value!r}"},
                    })
                    continue
                header = parse_fragment_header(frame.raw)
                if isinstance(header, ParseError):
                    await ws.send_json({"type": "error", "error": header.to_dict()})
                    continue
                # 连接路径已绑定设备身份：帧内 device_id 必须与连接一致，
                # 防止一条连接伪造其它设备的标识上报
                if header.device_id != device_id:
                    await ws.send_json({
                        "type": "error",
                        "error": {
                            "reason": "DEVICE_ID_MISMATCH",
                            "detail": f"connection is {device_id!r} but frame says "
                                      f"{header.device_id!r}",
                        },
                    })
                    continue
                result = gateway.handle_fragment(
                    header.device_id, header.seq, header.index,
                    header.total, header.version, header.payload_fragment,
                )
                await ws.send_json(_ingest_to_dict(result))
        except WebSocketDisconnect:
            pass
        except Exception as exc:  # 传输层异常不应拖垮网关
            try:
                await ws.send_json({"type": "error",
                                    "error": {"reason": "INTERNAL_ERROR", "detail": str(exc)}})
            except Exception:
                pass
        finally:
            await registry.remove(device_id, ws)
            # 未重组完成的报文视为残缺：设备重连后依据 resume 游标重传
            gateway.abort_pending(device_id)

    return app
