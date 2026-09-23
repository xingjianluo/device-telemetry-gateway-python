"""设备接入网关入口。

核心逻辑位于 :mod:`gateway` 包；``main`` 仅提供 HTTP/WS 传输层。
"""

from fastapi import FastAPI

from gateway.core import Gateway
from gateway.registry import DeviceRegistry
from gateway.transport import create_app


registry = DeviceRegistry()
gateway = Gateway(registry)
app: FastAPI = create_app(gateway)


@app.get("/")
def read_root() -> dict:
    return {"service": "device-telemetry-gateway", "status": "ok"}
