"""网关门面：组合 重组/校验/去重落库/续传/指令下发。"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field

from gateway.commandstore import CommandMetrics, CommandStatus
from gateway.commands import Command, CommandExecutor, CommandResult
from gateway.protocol import (
    R_DEVICE_UNKNOWN,
    R_DUPLICATE_SEQ_CONFLICT,
    R_FRAGMENT_GAP,
    R_MALFORMED_JSON,
    R_NOT_AN_OBJECT,
    ParseError,
)
from gateway.reassembly import Reassembler
from gateway.session import DeviceSession
from gateway.telemetry import TelemetryRecord, normalize_telemetry


@dataclass
class IngestResult:
    device_id: str
    seq: int
    accepted: bool
    duplicate: bool = False  # 该 seq 此前已确认/落库，本次幂等确认，未产生新业务记录
    complete: bool = False   # 本分片是否促成整条报文完成
    record: TelemetryRecord | None = None
    error: ParseError | None = None


class Gateway:
    def __init__(
        self,
        executor: CommandExecutor,
        known_devices: frozenset[str] = frozenset(),
    ) -> None:
        self._executor = executor
        self._known = set(known_devices)
        self._reassembler = Reassembler()
        self._sessions: dict[str, DeviceSession] = {}
        # (device_id, seq) -> 已落库记录；同 seq 只允许一条业务记录
        self._records: dict[tuple[str, int], TelemetryRecord] = {}
        self._lock = threading.Lock()

    # ---- 会话与续传 ----
    def register_device(self, device_id: str) -> DeviceSession:
        with self._lock:
            return self._get_or_create_locked(device_id)

    def _get_or_create_locked(self, device_id: str) -> DeviceSession:
        session = self._sessions.get(device_id)
        if session is None:
            session = DeviceSession(device_id=device_id)
            self._sessions[device_id] = session
            self._known.add(device_id)
        return session

    def get_or_create_session(self, device_id: str) -> DeviceSession:
        with self._lock:
            return self._get_or_create_locked(device_id)

    def resume_state(self, device_id: str) -> dict[str, object]:
        """供重连设备续传：返回已确认游标与已确认序列。

        设备依据 confirmed_seq 只重传未确认数据；已确认数据即使重传也不会重复落库。
        """
        with self._lock:
            session = self._sessions.get(device_id)
            if session is None:
                return {"known": False, "confirmed_seq": -1, "confirmed_seqs": []}
            return {
                "known": True,
                "confirmed_seq": session.confirmed_seq,
                "confirmed_seqs": sorted(session.confirmed_seqs),
            }

    def abort_pending(self, device_id: str) -> list[ParseError]:
        """连接中断等原因放弃该设备所有未重组完成的报文：明确拒绝为 FRAGMENT_GAP。"""
        # Reassembler 不暴露内部快照遍历，这里按 drop 清理由调用方已知的 seq；
        # 为支持整连接清理，Reassembler 提供 drop_device。
        return self._reassembler.drop_device(device_id)

    # ---- 分片上报 ----
    def handle_fragment(
        self,
        device_id: str,
        seq: int,
        index: int,
        total: int,
        version: str,
        chunk: str,
    ) -> IngestResult:
        with self._lock:
            if device_id not in self._known:
                return IngestResult(
                    device_id, seq, accepted=False,
                    error=ParseError(R_DEVICE_UNKNOWN, f"device {device_id!r} not registered"),
                )
            session = self._get_or_create_locked(device_id)

            # 续传幂等：已确认 seq 的重复上报直接 ACK，绝不产生第二条业务记录
            if session.is_confirmed(seq):
                return IngestResult(
                    device_id, seq, accepted=True, duplicate=True,
                    record=self._records.get((device_id, seq)),
                )

            outcome = self._reassembler.add_fragment(
                device_id, seq, index, total, version, chunk
            )
            if isinstance(outcome, ParseError):
                return IngestResult(device_id, seq, accepted=False, error=outcome)
            if outcome is None:
                return IngestResult(device_id, seq, accepted=True, complete=False)

            # 整条报文重组完成 -> 解析 + 版本校验 + 落库
            text = outcome.assembled()
            try:
                payload = json.loads(text)
            except (json.JSONDecodeError, ValueError) as exc:
                return IngestResult(
                    device_id, seq, accepted=False, complete=True,
                    error=ParseError(R_MALFORMED_JSON, f"reassembled payload: {exc}"),
                )
            if not isinstance(payload, dict):
                return IngestResult(
                    device_id, seq, accepted=False, complete=True,
                    error=ParseError(R_NOT_AN_OBJECT, "payload must be a json object"),
                )
            record_or_err = normalize_telemetry(device_id, seq, version, payload)
            if isinstance(record_or_err, ParseError):
                return IngestResult(
                    device_id, seq, accepted=False, complete=True, error=record_or_err
                )

            existing = self._records.get((device_id, seq))
            if existing is not None:
                if existing.raw == record_or_err.raw:
                    # 理论上前面 is_confirmed 已挡住；保留为一致性防线
                    return IngestResult(
                        device_id, seq, accepted=True, duplicate=True, record=existing
                    )
                return IngestResult(
                    device_id, seq, accepted=False, complete=True,
                    error=ParseError(
                        R_DUPLICATE_SEQ_CONFLICT,
                        f"seq={seq} reused with different payload",
                    ),
                )

            self._records[(device_id, seq)] = record_or_err
            session.mark_confirmed(seq)
            return IngestResult(
                device_id, seq, accepted=True, complete=True, record=record_or_err
            )

    def stored_records(self, device_id: str) -> list[TelemetryRecord]:
        with self._lock:
            keys = sorted(k for k in self._records if k[0] == device_id)
            return [self._records[k] for k in keys]

    # ---- 下行指令 ----
    def submit_command(self, command: Command) -> CommandResult:
        return self._executor.submit(command)

    def query_command(self, command_id: str) -> CommandStatus | ParseError:
        return self._executor.query_status(command_id)

    def cancel_command(self, command_id: str) -> CommandResult:
        return self._executor.cancel(command_id)

    def command_metrics(self) -> CommandMetrics:
        return self._executor.metrics()
