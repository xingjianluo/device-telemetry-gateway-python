"""装配默认网关并暴露 FastAPI 应用。

业务指令处理器在真实部署中替换为设备控制实现；
授权表 authorizations 描述每个设备被授权的指令集合。
"""
from __future__ import annotations

from gateway.app import create_app
from gateway.commands import CommandExecutor
from gateway.gateway import Gateway

# 设备 -> 被授权指令
_AUTHORIZATIONS = {
    "device-001": frozenset({"reboot", "set_interval"}),
    "device-002": frozenset({"set_interval"}),
}


def _reboot(device_id: str, args: dict) -> dict:
    return {"rebooted": True, "device_id": device_id}


def _set_interval(device_id: str, args: dict) -> dict:
    seconds = int(args["seconds"])
    return {"interval_seconds": seconds}


def build_gateway() -> Gateway:
    executor = CommandExecutor(
        authorizations=_AUTHORIZATIONS,
        handlers={"reboot": _reboot, "set_interval": _set_interval},
        max_queue=16,
    )
    return Gateway(executor=executor, known_devices=frozenset(_AUTHORIZATIONS))


gateway = build_gateway()
app = create_app(gateway)
