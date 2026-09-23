"""网关门面：串联帧解析、重组、会话去重与指令下发。

不依赖任何网络框架，输入输出均为普通对象，便于在单测中以
"输入报文 -> 判定依据 -> 最终状态" 的方式验证。
"""

from __future__ import annotations

import threading

from .assembler import Reassembler
from .codec import decode_payload, parse_frame
from .commands import CommandBus, CommandHandler
from .errors import GatewayError, RejectCode
from .protocol import (
    AcceptResult,
    CommandRequest,
    CommandResult,
    Frame,
    MessageType,
)
from .registry import DeviceRegistry
from .session import SessionState


class Gateway:
    def __init__(
        self,
        registry: DeviceRegistry | None = None,
        *,
        max_pending: int = 16,
        command_handler: CommandHandler | None = None,
    ) -> None:
        self.registry = registry or DeviceRegistry()
        self.reassembler = Reassembler()
        self.command_bus = CommandBus(
            self.registry, max_pending=max_pending, handler=command_handler
        )
        self._sessions: dict[str, SessionState] = {}
        self._lock = threading.Lock()

    # ---- 内部辅助 -----------------------------------------------------

    @staticmethod
    def _reject(frame: Frame | None, code: RejectCode, reason: str) -> AcceptResult:
        return AcceptResult(
            accepted=False,
            device_id=frame.device_id if frame else None,
            session_id=frame.session_id if frame else None,
            seq=frame.seq if frame else None,
            code=code,
            reason=reason,
        )

    def _bind_session(self, frame: Frame, resume_from: int | None) -> SessionState:
        """按 device 取/建会话，并处理断线重连的续传绑定。"""
        with self._lock:
            state = self._sessions.get(frame.device_id)
            claimed = frame.resume_from if resume_from is None else resume_from
            if state is None:
                if claimed is None:
                    claimed = 0
                state = SessionState(frame.device_id, frame.session_id, claimed)
                self._sessions[frame.device_id] = state
                return state

            if frame.session_id != state.session_id:
                # 新的 session_id 视为断线重连：必须携带与网关一致的位点。
                if claimed is None:
                    raise GatewayError(
                        f"设备 {frame.device_id} 以新会话 {frame.session_id} 重连，"
                        "必须携带 resume_from 以支持续传",
                        code=RejectCode.RESUME_FROM_MISMATCH,
                    )
                state.check_resume(claimed)
                self.reassembler.drop_session(state.session_id)
                state.rebind(frame.session_id)
            elif claimed is not None:
                state.check_resume(claimed)
            return state

    # ---- 入站 ---------------------------------------------------------

    def handle_frame(self, raw: str | bytes | dict, *, resume_from: int | None = None) -> AcceptResult:
        """处理一帧输入（遥测分片 / ACK），返回可解释结果。"""
        frame: Frame | None = None
        try:
            frame = parse_frame(raw)
            if not self.registry.is_known(frame.device_id):
                return self._reject(
                    frame,
                    RejectCode.DEVICE_UNKNOWN,
                    f"未注册设备: {frame.device_id}",
                )
            state = self._bind_session(frame, resume_from)

            if frame.message_type is MessageType.ACK:
                return self._handle_ack(frame, state)

            # 遥测：frame.fragment 已由 parse_frame 保证非 None
            assert frame.fragment is not None
            # 已完成的 seq 重放（任一分片）直接判重，不再进入重组缓存，
            # 也不产生新记录。
            if state.is_duplicate(frame.seq):
                return AcceptResult(
                    accepted=True,
                    device_id=frame.device_id,
                    session_id=frame.session_id,
                    seq=frame.seq,
                    completed=True,
                    duplicate=True,
                    reason=f"seq={frame.seq} 已处理过，重复上报已去重，不产生新记录",
                )
            completed, payload_bytes = self.reassembler.add(
                frame.session_id, frame.seq, frame.fragment
            )
            if not completed:
                return AcceptResult(
                    accepted=True,
                    device_id=frame.device_id,
                    session_id=frame.session_id,
                    seq=frame.seq,
                    completed=False,
                    reason="分片已接收，等待其余分片",
                )

            assert payload_bytes is not None
            payload = decode_payload(frame.version, payload_bytes)
            record = {
                "version": frame.version,
                "seq": frame.seq,
                "session_id": frame.session_id,
                "payload": payload,
            }
            stored = state.accept(frame.seq, record)
            if not stored:  # 并发兜底：同 seq 最后一片被其他线程抢先完成
                return AcceptResult(
                    accepted=True,
                    device_id=frame.device_id,
                    session_id=frame.session_id,
                    seq=frame.seq,
                    completed=True,
                    duplicate=True,
                    reason=f"seq={frame.seq} 并发重复，已去重",
                )
            return AcceptResult(
                accepted=True,
                device_id=frame.device_id,
                session_id=frame.session_id,
                seq=frame.seq,
                completed=True,
                record=record,
                reason="新业务记录已写入",
            )
        except GatewayError as exc:
            return self._reject(frame, exc.code, str(exc))

    def _handle_ack(self, frame: Frame, state: SessionState) -> AcceptResult:
        if frame.fragment is not None:
            return self._reject(
                frame, RejectCode.BAD_FIELD_TYPE, "ACK 报文不得携带 frag/payload_b64"
            )
        fresh = state.confirm(frame.seq)
        return AcceptResult(
            accepted=True,
            device_id=frame.device_id,
            session_id=frame.session_id,
            seq=frame.seq,
            completed=True,
            duplicate=not fresh,
            reason=(
                f"已确认位点推进至 {frame.seq}"
                if fresh
                else f"seq={frame.seq} 的 ACK 重复，位点未变化"
            ),
        )

    def on_disconnect(self, device_id: str) -> None:
        """设备断开：标记离线但保留已确认位点与业务记录，等待续传。"""
        with self._lock:
            state = self._sessions.get(device_id)
            if state is not None:
                self.reassembler.drop_session(state.session_id)
                state.online = False

    # ---- 出站 ---------------------------------------------------------

    async def send_command(self, req: CommandRequest) -> CommandResult:
        """提交下行指令。"""
        return await self.command_bus.submit(req)

    # ---- 观测 ---------------------------------------------------------

    def state_snapshot(self, device_id: str) -> dict | None:
        """导出设备状态（已确认位点、记录数、记录 seq 列表等）。"""
        with self._lock:
            state = self._sessions.get(device_id)
            return state.snapshot() if state else None
