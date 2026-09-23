"""分片重组：支持乱序到达、重复分片去重、残缺拒绝。"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from gateway.protocol import (
    R_DUPLICATE_FRAGMENT_CONFLICT,
    R_FRAGMENT_GAP,
    R_FRAGMENT_HEADER_CONFLICT,
    ParseError,
)


@dataclass
class PendingMessage:
    device_id: str
    seq: int
    total: int
    version: str
    parts: dict[int, str] = field(default_factory=dict)

    def is_complete(self) -> bool:
        return len(self.parts) == self.total

    def assembled(self) -> str:
        # 调用方保证 is_complete()；按 index 顺序拼接
        return "".join(self.parts[i] for i in range(self.total))


class Reassembler:
    """按 (device_id, seq) 缓冲分片；线程安全。"""

    def __init__(self) -> None:
        self._pending: dict[tuple[str, int], PendingMessage] = {}
        self._lock = threading.Lock()

    def add_fragment(
        self,
        device_id: str,
        seq: int,
        index: int,
        total: int,
        version: str,
        chunk: str,
    ) -> PendingMessage | ParseError | None:
        """返回重组完成的 PendingMessage；未完成返回 None；错误返回 ParseError。

        - 乱序：按 index 落位，全部到齐即完成；
        - 重复：同一 (seq,index) 内容一致 -> 幂等忽略；内容不一致 -> 拒绝；
        - total/version 在同一 seq 的多个分片间必须一致，否则拒绝。
        """
        with self._lock:
            key = (device_id, seq)
            pending = self._pending.get(key)
            if pending is None:
                pending = PendingMessage(
                    device_id=device_id, seq=seq, total=total, version=version
                )
                self._pending[key] = pending
            else:
                if pending.total != total:
                    return ParseError(
                        R_FRAGMENT_HEADER_CONFLICT,
                        f"seq={seq} total changed: {pending.total} -> {total}",
                    )
                if pending.version != version:
                    return ParseError(
                        R_FRAGMENT_HEADER_CONFLICT,
                        f"seq={seq} version changed: {pending.version} -> {version}",
                    )
            existing = pending.parts.get(index)
            if existing is not None:
                if existing == chunk:
                    # 完全相同的重复分片：幂等，不产生重复记录
                    return pending if pending.is_complete() else None
                return ParseError(
                    R_DUPLICATE_FRAGMENT_CONFLICT,
                    f"seq={seq} index={index} retransmitted with different payload",
                )
            pending.parts[index] = chunk
            if pending.is_complete():
                # 取走即出队，保证同一 seq 只完成一次
                return self._pending.pop(key)
            return None

    def drop(self, device_id: str, seq: int) -> None:
        with self._lock:
            self._pending.pop((device_id, seq), None)

    def drop_device(self, device_id: str) -> list[ParseError]:
        """放弃该设备所有未完成报文（如连接中断），逐条给出 FRAGMENT_GAP 拒绝原因。"""
        errors: list[ParseError] = []
        with self._lock:
            keys = [k for k in self._pending if k[0] == device_id]
            for key in keys:
                pending = self._pending.pop(key)
                missing = sorted(set(range(pending.total)) - set(pending.parts))
                errors.append(
                    ParseError(
                        R_FRAGMENT_GAP,
                        f"seq={pending.seq} abandoned, missing fragments={missing}",
                    )
                )
        return errors
