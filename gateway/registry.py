"""设备与指令授权注册表。"""

from __future__ import annotations

from .errors import RejectCode


class DeviceRegistry:
    def __init__(self) -> None:
        self._devices: dict[str, set[str]] = {}

    def register(self, device_id: str, allowed_commands: set[str]) -> None:
        """注册设备及其被授权的指令集合。"""
        self._devices[device_id] = set(allowed_commands)

    def is_known(self, device_id: str) -> bool:
        return device_id in self._devices

    def authorize(self, device_id: str, command: str) -> tuple[bool, RejectCode | None]:
        """判断设备是否可执行某指令，返回 (是否允许, 拒绝码)。

        设备不存在与越权是两种可区分的失败：
        ``device_unknown`` vs ``unauthorized_command``。
        """
        allowed = self._devices.get(device_id)
        if allowed is None:
            return False, RejectCode.DEVICE_UNKNOWN
        if command not in allowed:
            return False, RejectCode.UNAUTHORIZED_COMMAND
        return True, None
