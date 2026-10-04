"""装配默认网关并暴露 FastAPI 应用。

业务指令处理器在真实部署中替换为设备控制实现；
授权表 authorizations 描述每个设备被授权的指令集合。

容量相关参数均可通过环境变量配置（见 build_gateway）：
  GATEWAY_MAX_QUEUE    每设备在途+排队积压上限（默认 16）
  GATEWAY_MAX_HISTORY  终态指令结果保留条数，FIFO 淘汰（默认 256）
  GATEWAY_MAX_RETIRED  已淘汰指令墓碑保留条数，环形有界（默认 4096）
"""
from __future__ import annotations

import logging
import os

from gateway.app import create_app
from gateway.commands import CommandExecutor
from gateway.gateway import Gateway

logging.basicConfig(
    level=os.environ.get("GATEWAY_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw is not None else default


def build_gateway() -> Gateway:
    executor = CommandExecutor(
        authorizations=_AUTHORIZATIONS,
        handlers={"reboot": _reboot, "set_interval": _set_interval},
        max_queue=_env_int("GATEWAY_MAX_QUEUE", 16),
        max_history=_env_int("GATEWAY_MAX_HISTORY", 256),
        max_retired=_env_int("GATEWAY_MAX_RETIRED", 4096),
    )
    return Gateway(executor=executor, known_devices=frozenset(_AUTHORIZATIONS))


gateway = build_gateway()
app = create_app(gateway)
