"""指令状态登记册：有界保存指令终态，支撑事后查询、撤销与容量对账。

职责边界（不负责鉴权、不负责执行、不维护每设备 FIFO，那些在 commands.py）：
  * 以 command_id 为键登记每条提交的指令，给出可区分、可重复查询的稳定终态；
  * 撤销仅允许在"尚未开始执行"时生效，状态只能向前流转，不可回退；
  * 终态记录数量有明确上限：满了先把最旧终态压缩成墓碑（只留结论不留响应），
    墓碑也到上限就彻底遗忘最旧的一条；连可清理终态都没有时拒绝新提交
    （COMMAND_STATUS_LIMIT），绝不无界增长；
  * 所有提交/受理/复用/各类拒绝/撤销/清理都计数，可对平两张账（见 check_invariants）。

command_id 是全局幂等键：首次提交的结论（包括"提交即拒"）绑定该号；
之后同号提交：同设备 -> 复用旧结论（duplicate=true，绝不二次执行）；
不同设备 -> COMMAND_DUPLICATE。超窗被彻底遗忘后，该号视为从未出现。
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Any

from gateway.protocol import (
    R_COMMAND_CANCEL_TOO_LATE,
    R_COMMAND_CANCELED,
    R_COMMAND_NOT_FOUND,
    R_COMMAND_STATUS_LIMIT,
    ParseError,
)


class CommandStatus(str, Enum):
    """指令生命周期状态。只能向后流转，不存在回退边。"""

    PENDING = "pending"      # 已受理，排在设备队列里，尚未开始执行（唯一可撤销窗口）
    EXECUTING = "executing"  # 已被工作线程取出并开始执行（撤销窗口已关闭）
    SUCCEEDED = "succeeded"  # 终态：执行成功
    REJECTED = "rejected"    # 终态：被拒绝（鉴权/队列满/无处理器/处理器异常等）
    CANCELED = "canceled"    # 终态：在开始执行前被撤销


# 可被清理的终态
TERMINAL_STATUSES = frozenset(
    {CommandStatus.SUCCEEDED, CommandStatus.REJECTED, CommandStatus.CANCELED}
)


@dataclass(frozen=True)
class CommandStatusView:
    """一次查询/复用的结果视图：终态稳定，重复查询返回内容一致。"""

    command_id: str
    device_id: str
    name: str
    status: str  # CommandStatus 的值
    reason: ParseError | None = None
    response: Any = None          # 完整终态记录可能携带执行响应；墓碑中为 None
    duplicate: bool = False       # True：本次复用了该指令号此前的结论


@dataclass(frozen=True)
class CancelResult:
    """撤销结论：成功 / 幂等 / 太晚 / 未知，均可区分。"""

    command_id: str
    canceled: bool            # 该指令最终是否处于 canceled
    idempotent: bool = False  # True：此前已被撤销，本次为重复撤销
    reason: ParseError | None = None


@dataclass
class CommandRecord:
    """登记册内部活动记录（完整记录）。终态后可被压缩成墓碑。"""

    command_id: str
    device_id: str
    name: str
    status: CommandStatus
    seq: int
    reason: ParseError | None = None
    response: Any = None
    event: threading.Event | None = None


@dataclass(frozen=True)
class _Tombstone:
    """被压缩的终态：只留可区分结论，不留响应体。仍占据去重窗口。"""

    device_id: str
    name: str
    status: CommandStatus
    reason: ParseError | None = None


@dataclass(frozen=True)
class Precheck:
    """一次提交的原子判定（鉴权表为不可变只读数据，鉴权结论在锁外算好后传入）：
      kind="new_pending"   全新 command_id 且鉴权通过：record 已为 PENDING 并计入
                           accepted，可放入设备 FIFO；
      kind="new_rejected"  全新 command_id 但鉴权即拒：view 即稳定终态（不可重提覆盖）；
      kind="reuse_live"    同设备同号复用活动记录：等待其终态即可，绝不二次执行；
      kind="reuse_tomb"    同设备同号命中墓碑窗口：直接复用旧结论（duplicate）；
      kind="cross_device"  该号已被其它设备占用 -> COMMAND_DUPLICATE；
      kind="limit"         容量到顶且无可清理终态 -> COMMAND_STATUS_LIMIT。
    """

    kind: str
    record: CommandRecord | None = None
    view: CommandStatusView | None = None


@dataclass
class CapacityMetrics:
    """容量与受理对账计数（累计值，除 active 外不因清理而减少）。"""

    submitted: int = 0
    accepted: int = 0                 # 进入设备队列的新指令数
    reused_duplicate: int = 0         # 同设备同号复用旧结论的提交次数
    rejected_device_unknown: int = 0
    rejected_unauthorized: int = 0
    rejected_duplicate: int = 0       # 跨设备占用 command_id
    rejected_status_limit: int = 0
    rejected_queue_full: int = 0      # 已受理但设备队列放不下（受理后终态拒绝）
    terminal_succeeded: int = 0
    terminal_rejected_unsupported: int = 0
    terminal_rejected_handler_error: int = 0
    terminal_canceled: int = 0
    cancel_too_late: int = 0
    cancel_not_found: int = 0
    query_found: int = 0
    query_not_found: int = 0
    evicted_compacted: int = 0       # 完整记录被压缩成墓碑
    evicted_forgotten: int = 0       # 墓碑超出窗口被彻底遗忘

    def pre_rejected_total(self) -> int:
        """受理之前即被拒（对账恒等式①的右侧分类之一）。"""
        return (
            self.rejected_device_unknown
            + self.rejected_unauthorized
            + self.rejected_duplicate
            + self.rejected_status_limit
        )

    def terminal_total(self) -> int:
        return (
            self.terminal_succeeded
            + self.terminal_rejected_unsupported
            + self.terminal_rejected_handler_error
            + self.terminal_canceled
            + self.rejected_queue_full
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "submitted": self.submitted,
            "accepted": self.accepted,
            "reused_duplicate": self.reused_duplicate,
            "rejected_device_unknown": self.rejected_device_unknown,
            "rejected_unauthorized": self.rejected_unauthorized,
            "rejected_duplicate": self.rejected_duplicate,
            "rejected_status_limit": self.rejected_status_limit,
            "rejected_queue_full": self.rejected_queue_full,
            "terminal_succeeded": self.terminal_succeeded,
            "terminal_rejected_unsupported": self.terminal_rejected_unsupported,
            "terminal_rejected_handler_error": self.terminal_rejected_handler_error,
            "terminal_canceled": self.terminal_canceled,
            "cancel_too_late": self.cancel_too_late,
            "cancel_not_found": self.cancel_not_found,
            "query_found": self.query_found,
            "query_not_found": self.query_not_found,
            "evicted_compacted": self.evicted_compacted,
            "evicted_forgotten": self.evicted_forgotten,
        }


# 预检后（受理前）拒绝原因 -> 计量字段
_PRE_REJECT_COUNTERS = {
    "DEVICE_UNKNOWN": "rejected_device_unknown",
    "COMMAND_UNAUTHORIZED": "rejected_unauthorized",
}
# 执行期拒绝原因 -> 计量字段
_EXEC_REJECT_COUNTERS = {
    "COMMAND_UNSUPPORTED": "terminal_rejected_unsupported",
    "COMMAND_HANDLER_ERROR": "terminal_rejected_handler_error",
}


class CommandRegistry:
    """有界指令状态登记册。线程安全（单把锁 + 每条活动记录一个 Event）。"""

    def __init__(self, max_records: int = 1024) -> None:
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        self._max = max_records
        self._records: dict[str, CommandRecord] = {}
        # 有界去重窗口：command_id -> 压缩结论；插入序即最旧->最新
        self._tombstones: OrderedDict[str, _Tombstone] = OrderedDict()
        self._lock = threading.Lock()
        self._seq = 0
        self._metrics = CapacityMetrics()

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _bump(self, name: str) -> None:
        setattr(self._metrics, name, getattr(self._metrics, name) + 1)

    def _view(self, record: CommandRecord, duplicate: bool = False) -> CommandStatusView:
        return CommandStatusView(
            command_id=record.command_id,
            device_id=record.device_id,
            name=record.name,
            status=record.status.value,
            reason=record.reason,
            response=record.response,
            duplicate=duplicate,
        )

    def _tomb_view(self, command_id: str, t: _Tombstone,
                   duplicate: bool = False) -> CommandStatusView:
        return CommandStatusView(
            command_id=command_id,
            device_id=t.device_id,
            name=t.name,
            status=t.status.value,
            reason=t.reason,
            response=None,
            duplicate=duplicate,
        )

    def _evict_one_locked(self) -> bool:
        """腾出一个槽位：压缩并移除最旧的完整终态记录。

        清理规则（确定、可解释）：
          1. 只清理终态记录；PENDING/EXECUTING 绝不可能被当成不存在；
          2. 按记录序号（≈首次提交先后）取最旧的一条；
          3. 完整记录 -> 墓碑（结论仍可查、同号仍去重）；墓碑窗口也满时，
             最旧墓碑被彻底遗忘。
        """
        victim: CommandRecord | None = None
        for record in self._records.values():
            if record.status in TERMINAL_STATUSES:
                victim = record
                break
        if victim is None:
            return False  # 全是未结束指令：不能清理，拒绝新提交而非误删
        del self._records[victim.command_id]
        # 同一 command_id 若已有更旧墓碑（正常不会），先移除再放到窗口最新端
        self._tombstones.pop(victim.command_id, None)
        self._tombstones[victim.command_id] = _Tombstone(
            device_id=victim.device_id,
            name=victim.name,
            status=victim.status,
            reason=victim.reason,
        )
        self._bump("evicted_compacted")
        while len(self._tombstones) > self._max:
            self._tombstones.popitem(last=False)
            self._bump("evicted_forgotten")
        return True

    # ------------------------------------------------------------------
    # 提交流程（与 commands.py 的执行器配合）
    # ------------------------------------------------------------------
    def submit_decision(
        self,
        command_id: str,
        device_id: str,
        name: str,
        auth_error: ParseError | None,
    ) -> Precheck:
        """提交入口的唯一原子判定。每次调用都计入 submitted。

        auth_error 由执行器查不可变授权表得出（DEVICE_UNKNOWN /
        COMMAND_UNAUTHORIZED / None）；决策与记账在此一把锁内同时完成，
        因此"预留记录"与"鉴权结论落盘"之间不存在撤销窗口，账始终平。
        """
        with self._lock:
            self._bump("submitted")

            live = self._records.get(command_id)
            if live is not None:
                if live.device_id != device_id:
                    self._bump("rejected_duplicate")
                    return Precheck(
                        "cross_device",
                        view=self._view(live),
                    )
                self._bump("reused_duplicate")
                return Precheck("reuse_live", record=live)

            tomb = self._tombstones.get(command_id)
            if tomb is not None:
                if tomb.device_id != device_id:
                    self._bump("rejected_duplicate")
                    return Precheck(
                        "cross_device", view=self._tomb_view(command_id, tomb)
                    )
                self._bump("reused_duplicate")
                return Precheck(
                    "reuse_tomb",
                    view=self._tomb_view(command_id, tomb, duplicate=True),
                )

            # 全新 command_id
            if auth_error is not None:
                # 容量受理（被拒记录也必须可查询）；满到连可清理终态都没有时，
                # 明确告知 STATUS_LIMIT 而不是含糊地丢失拒绝结论
                while len(self._records) >= self._max:
                    if not self._evict_one_locked():
                        self._bump("rejected_status_limit")
                        return Precheck(
                            "limit",
                            view=CommandStatusView(
                                command_id, device_id, name,
                                CommandStatus.REJECTED.value,
                                reason=ParseError(
                                    R_COMMAND_STATUS_LIMIT,
                                    f"command registry limit {self._max} reached and "
                                    f"no terminal record is evictable",
                                ),
                            ),
                        )
                self._seq += 1
                record = CommandRecord(
                    command_id=command_id, device_id=device_id, name=name,
                    status=CommandStatus.REJECTED, seq=self._seq,
                    reason=auth_error, event=threading.Event(),
                )
                self._records[command_id] = record
                counter = _PRE_REJECT_COUNTERS.get(auth_error.reason)
                if counter is not None:
                    self._bump(counter)
                record.event.set()
                return Precheck("new_rejected", view=self._view(record))

            # 鉴权通过：腾出容量后原子置为 PENDING 并计入 accepted
            while len(self._records) >= self._max:
                if not self._evict_one_locked():
                    self._bump("rejected_status_limit")
                    return Precheck(
                        "limit",
                        view=CommandStatusView(
                            command_id, device_id, name,
                            CommandStatus.REJECTED.value,
                            reason=ParseError(
                                R_COMMAND_STATUS_LIMIT,
                                f"command registry limit {self._max} reached and "
                                f"no terminal record is evictable",
                            ),
                        ),
                    )
            self._seq += 1
            record = CommandRecord(
                command_id=command_id, device_id=device_id, name=name,
                status=CommandStatus.PENDING, seq=self._seq,
                event=threading.Event(),
            )
            self._records[command_id] = record
            self._bump("accepted")
            return Precheck("new_pending", record=record)

    def reject_queue_full(self, command_id: str, max_queue: int) -> CommandStatusView:
        """已受理但设备 FIFO 放不下：PENDING -> REJECTED(QUEUE_FULL)。

        与撤销竞争时以撤销为准（撤销先行则返回撤销终态），绝不重复记账。
        """
        with self._lock:
            record = self._records.get(command_id)
            if record is None:
                # 极端情况下记录已被清理：给出稳定的队列满结论
                return CommandStatusView(
                    command_id, "", "", CommandStatus.REJECTED.value,
                    reason=ParseError(
                        "COMMAND_QUEUE_FULL",
                        f"device backlog limit {max_queue} reached and record aged out",
                    ),
                )
            if record.status is CommandStatus.CANCELED:
                return self._view(record)
            if record.status is not CommandStatus.PENDING:
                return self._view(record)
            record.status = CommandStatus.REJECTED
            record.reason = ParseError(
                "COMMAND_QUEUE_FULL",
                f"backlog limit {max_queue} reached for {record.device_id!r}",
            )
            self._bump("rejected_queue_full")
            assert record.event is not None
            record.event.set()
            return self._view(record)

    def begin_execution(self, command_id: str) -> bool:
        """工作线程取到指令后调用：真正开始执行。

        返回 True  => PENDING -> EXECUTING，应当执行处理器；
        返回 False => 指令在排队期间已被撤销（或已被拒），必须跳过，绝不执行。
        """
        with self._lock:
            record = self._records.get(command_id)
            if record is None or record.status is not CommandStatus.PENDING:
                return False
            record.status = CommandStatus.EXECUTING
            return True

    def finish_succeeded(self, command_id: str, response: Any) -> None:
        with self._lock:
            record = self._records.get(command_id)
            if record is None or record.status is not CommandStatus.EXECUTING:
                return
            record.status = CommandStatus.SUCCEEDED
            record.response = response
            self._bump("terminal_succeeded")
            assert record.event is not None
            record.event.set()

    def finish_exec_rejected(self, command_id: str, reason: ParseError) -> None:
        """执行期拒绝：无处理器 / 处理器抛异常。"""
        with self._lock:
            record = self._records.get(command_id)
            if record is None or record.status is not CommandStatus.EXECUTING:
                return
            record.status = CommandStatus.REJECTED
            record.reason = reason
            counter = _EXEC_REJECT_COUNTERS.get(reason.reason)
            if counter is not None:
                self._bump(counter)
            assert record.event is not None
            record.event.set()

    def wait_terminal_view(
        self, record: CommandRecord, duplicate: bool = True
    ) -> CommandStatusView:
        """锁外阻塞到终态，再回锁取快照（终态不可变，重复等待结论一致）。

        并发同号复用调用 duplicate=True；原始提交者自己等待时传 False。
        """
        assert record.event is not None
        record.event.wait()
        with self._lock:
            live = self._records.get(record.command_id)
            if live is not None:
                return self._view(live, duplicate=duplicate)
            tomb = self._tombstones.get(record.command_id)
            if tomb is not None:
                return self._tomb_view(record.command_id, tomb, duplicate=duplicate)
            # 等待期间被彻底遗忘：给出稳定的明确结论而非含糊异常
            return CommandStatusView(
                record.command_id, record.device_id, record.name,
                CommandStatus.CANCELED.value,
                reason=ParseError(
                    R_COMMAND_CANCELED, "terminal record aged out while waiting",
                ),
                duplicate=duplicate,
            )

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def query(self, command_id: str) -> CommandStatusView | None:
        """凭指令号查终态；从未见过 / 已彻底遗忘 -> None（可区分、明确）。"""
        with self._lock:
            live = self._records.get(command_id)
            if live is not None:
                self._bump("query_found")
                return self._view(live)
            tomb = self._tombstones.get(command_id)
            if tomb is not None:
                self._bump("query_found")
                return self._tomb_view(command_id, tomb)
            self._bump("query_not_found")
            return None

    # ------------------------------------------------------------------
    # 撤销
    # ------------------------------------------------------------------
    def cancel(self, command_id: str) -> CancelResult:
        """撤销尚未开始执行的指令。状态机判定全部在同一把锁内原子完成。"""
        with self._lock:
            live = self._records.get(command_id)
            if live is not None:
                if live.status is CommandStatus.PENDING:
                    live.status = CommandStatus.CANCELED
                    live.reason = ParseError(
                        R_COMMAND_CANCELED,
                        "canceled before execution started",
                    )
                    self._bump("terminal_canceled")
                    assert live.event is not None
                    live.event.set()  # 唤醒提交者与并发同号等待者
                    return CancelResult(command_id, canceled=True)
                if live.status is CommandStatus.CANCELED:
                    return CancelResult(command_id, canceled=True, idempotent=True)
                self._bump("cancel_too_late")
                return CancelResult(
                    command_id, canceled=False,
                    reason=ParseError(
                        R_COMMAND_CANCEL_TOO_LATE,
                        f"command is {live.status.value!r}, cancellation refused",
                    ),
                )

            tomb = self._tombstones.get(command_id)
            if tomb is not None:
                if tomb.status is CommandStatus.CANCELED:
                    return CancelResult(command_id, canceled=True, idempotent=True)
                self._bump("cancel_too_late")
                return CancelResult(
                    command_id, canceled=False,
                    reason=ParseError(
                        R_COMMAND_CANCEL_TOO_LATE,
                        f"command already terminated as {tomb.status.value!r}",
                    ),
                )

            self._bump("cancel_not_found")
            return CancelResult(
                command_id, canceled=False,
                reason=ParseError(
                    R_COMMAND_NOT_FOUND,
                    "no such command_id: never submitted or already aged out",
                ),
            )

    # ------------------------------------------------------------------
    # 观测与对账
    # ------------------------------------------------------------------
    def metrics(self) -> CapacityMetrics:
        with self._lock:
            # 返回副本，避免外部读到变动中的计数
            return CapacityMetrics(**self._metrics.to_dict())

    def active_count(self) -> int:
        with self._lock:
            return sum(
                1 for r in self._records.values()
                if r.status not in TERMINAL_STATUSES
            )

    def stored_count(self) -> int:
        with self._lock:
            return len(self._records)

    def tombstone_count(self) -> int:
        with self._lock:
            return len(self._tombstones)

    def check_invariants(self) -> None:
        """对账与安全不变式；违例直接 AssertionError。

        ① 提交账：submitted == accepted + reused_duplicate
                  + 受理前各类拒绝(device_unknown/unauthorized/duplicate/status_limit)
           （queue_full 的指令已受理，归入 accepted，在②中落终态）
        ② 受理账：accepted == 当前未结束 + 各终态（成功/取消/无处理器/处理器异常/队列满）
        ③ 容量账：完整记录数、墓碑数均不超过 max_records；
                  未结束指令绝不在墓碑里、绝不可能被清理。
        """
        with self._lock:
            m = self._metrics
            active = sum(
                1 for r in self._records.values()
                if r.status not in TERMINAL_STATUSES
            )
            assert m.submitted == (
                m.accepted
                + m.reused_duplicate
                + m.pre_rejected_total()
            ), f"submit ledger mismatch: {m.to_dict()}"
            assert m.accepted == active + m.terminal_total(), (
                f"accepted ledger mismatch: accepted={m.accepted} "
                f"active={active} terminals={m.terminal_total()}"
            )
            assert len(self._records) <= self._max, "records exceed capacity"
            assert len(self._tombstones) <= self._max, "tombstones exceed window"
            for cid, tomb in self._tombstones.items():
                assert tomb.status in TERMINAL_STATUSES
                live = self._records.get(cid)
                assert live is None or live.status in TERMINAL_STATUSES
