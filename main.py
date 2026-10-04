"""装配默认网关并暴露 FastAPI 应用。

业务指令处理器在真实部署中替换为设备控制实现；
授权表 authorizations 描述每个设备被授权的指令集合。

容量上限可经环境变量配置（长时间不重启场景下约束状态规模）：
  GATEWAY_MAX_QUEUE          每设备在途+排队指令数（默认 16）
  GATEWAY_MAX_STATUS_RECORDS 指令状态登记册完整记录上限（默认 4096）；
                             满后按"终态压缩成墓碑、墓碑超窗遗忘"的规则清理。
"""
from __future__ import annotations

import os

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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def build_gateway() -> Gateway:
    executor = CommandExecutor(
        authorizations=_AUTHORIZATIONS,
        handlers={"reboot": _reboot, "set_interval": _set_interval},
        max_queue=_env_int("GATEWAY_MAX_QUEUE", 16),
        max_status_records=_env_int("GATEWAY_MAX_STATUS_RECORDS", 4096),
    )
    return Gateway(executor=executor, known_devices=frozenset(_AUTHORIZATIONS))


gateway = build_gateway()
app = create_app(gateway)
