"""分片重组：缓存不完整的遥测分片，集齐后还原完整 payload。

按 (session_id, seq) 聚合；规则：
- ``frag.n`` 一旦确定即固定，后续分片 n 不一致 -> ``fragment_total_mismatch``。
- 同一索引重复到达时内容必须完全一致，否则 -> ``fragment_conflict``；
  内容一致视为重传，幂等忽略。
- 每个分片的 ``payload_b64`` 独立做严格 base64 解码，非法 ->
  ``payload_not_base64``；集齐后把各分片的**字节**按索引顺序拼接
  （base64 文本不能跨分片直接拼接，因为 padding/3 字节分组边界）。
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field

from .errors import GatewayError, RejectCode
from .protocol import Fragment


def _err(code: RejectCode, message: str) -> GatewayError:
    return GatewayError(message, code=code)


def _decode_b64(seq: int, index: int, payload_b64: str) -> bytes:
    try:
        return base64.b64decode(payload_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise _err(
            RejectCode.PAYLOAD_NOT_BASE64,
            f"seq={seq} 索引 {index} 的分片不是合法 base64: {exc}",
        ) from exc


@dataclass
class _Pending:
    total: int
    chunks: dict[int, bytes] = field(default_factory=dict)


class Reassembler:
    def __init__(self) -> None:
        self._pending: dict[tuple[str, int], _Pending] = {}

    def add(
        self, session_id: str, seq: int, fragment: Fragment
    ) -> tuple[bool, bytes | None]:
        """放入一个分片。

        返回 ``(是否集齐, 集齐后的完整 payload 字节)``；未集齐时第二项为
        ``None``。索引/总数/编码冲突直接抛 :class:`GatewayError`。
        """
        key = (session_id, seq)
        pending = self._pending.get(key)
        if pending is None:
            pending = _Pending(total=fragment.total)
            self._pending[key] = pending
        elif pending.total != fragment.total:
            raise _err(
                RejectCode.FRAGMENT_TOTAL_MISMATCH,
                f"seq={seq} 分片总数冲突: 已声明 n={pending.total}，收到 n={fragment.total}",
            )

        chunk = _decode_b64(seq, fragment.index, fragment.payload_b64)
        existing = pending.chunks.get(fragment.index)
        if existing is not None:
            if existing != chunk:
                raise _err(
                    RejectCode.FRAGMENT_CONFLICT,
                    f"seq={seq} 索引 {fragment.index} 的分片重复但内容不一致",
                )
            # 内容一致：重传，幂等。
            return len(pending.chunks) == pending.total, None

        pending.chunks[fragment.index] = chunk
        if len(pending.chunks) == pending.total:
            assembled = b"".join(pending.chunks[i] for i in range(pending.total))
            del self._pending[key]
            return True, assembled
        return False, None

    def drop_session(self, session_id: str) -> None:
        """设备断开时清理该会话的未完成分片（已完成记录不受影响）。"""
        for key in [k for k in self._pending if k[0] == session_id]:
            del self._pending[key]

    def pending_count(self) -> int:
        """当前未集齐的分片组数量（诊断/测试用）。"""
        return len(self._pending)
