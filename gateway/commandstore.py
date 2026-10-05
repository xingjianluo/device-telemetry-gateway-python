"""指令状态注册表：可查询状态、取消判定、有界保留与可对账计数。

容量模型（全部可配置，均为“有效在途容量”之外的终态治理）：
  * max_history：终态指令结果的保留条数（FIFO 淘汰）；
  * max_retired：已淘汰指令的“墓碑”保留条数（环形有界），用于在保留窗内
    继续给出明确、可区分、稳定一致的答复，并防止同一 command_id 二次执行。
所有方法都假定在 CommandExecutor 的同一把锁内调用（本模块不自持锁）。
"""
from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from gateway.protocol import ParseError

logger = logging.getLogger("gateway.commands")


class CommandState(str, Enum):
    """指令生命周期状态。

    QUEUED -> RUNNING -> SUCCEEDED | FAILED
    QUEUED -> CANCELLED            （仅“尚未开始执行”可取消）
    REJECTED                       （提交那一刻即被拒：未授权/设备不存在/积压超限等，
                                    从未入队、从未执行）
    后四者为终态，状态只进不退。
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


TERMINAL_STATES = frozenset(
    {
        CommandState.SUCCEEDED,
        CommandState.FAILED,
        CommandState.CANCELLED,
        CommandState.REJECTED,
    }
)


@dataclass
class CommandRecord:
    """一条指令在状态机中的实时记录（非终态不得淘汰）。"""

    command_id: str
    device_id: str
    name: str
    state: CommandState = CommandState.QUEUED
    response: Any = None
    reason: ParseError | None = None
    event: threading.Event = field(default_factory=threading.Event)


@dataclass(frozen=True)
class CommandStatus:
    """一次指令状态查询的不可变快照（重复查询结果稳定一致）。"""

    command_id: str
    state: CommandState
    device_id: str
    name: str
    response: Any = None
    reason: ParseError | None = None
    retired: bool = False  # True：记录已按容量规则淘汰，state 为淘汰前终态


@dataclass
class CommandMetrics:
    """提交对账计数：submitted = accepted + duplicates + 各类被拒之和。"""

    submitted: int = 0
    accepted: int = 0
    duplicates: int = 0
    rejected: dict[str, int] = field(default_factory=dict)
    terminal: dict[str, int] = field(default_factory=dict)
    # 当前容量占用（非终态绝不因容量治理而消失）
    active: int = 0
    history: int = 0
    retired: int = 0

    def accounts_balanced(self) -> bool:
        return self.submitted == (
            self.accepted + self.duplicates + sum(self.rejected.values())
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "submitted": self.submitted,
            "accepted": self.accepted,
            "duplicates": self.duplicates,
            "rejected": dict(self.rejected),
            "terminal": dict(self.terminal),
            "active": self.active,
            "history": self.history,
            "retired": self.retired,
            "accounts_balanced": self.accounts_balanced(),
        }


class BoundedCommandStore:
    """指令状态注册表。

    不变量：
      * 非终态（QUEUED/RUNNING）记录绝不参与淘汰 —— 容量清理不会把还没结束
        的指令当成不存在；
      * 终态记录（含提交那一刻即被拒的 REJECTED）按完成先后进入长度
        max_history 的 FIFO 保留窗；超出即把最早的终态记录降级为墓碑；
        墓碑环 max_retired 有界，超出后最早的墓碑彻底遗忘；
      * 墓碑窗内的 command_id 再次提交一律 COMMAND_RETIRED 拒绝 —— 同一条指令
        不可能被重复执行；
      * 所有计数只在提交路径上每单恰好归类一次，保证
        submitted = accepted + duplicates + Σrejected。
    """

    def __init__(self, max_history: int = 256, max_retired: int = 4096) -> None:
        if max_history < 0 or max_retired < 0:
            raise ValueError("max_history/max_retired must be >= 0")
        self._max_history = max_history
        self._max_retired = max_retired
        # 活跃 + 仍在保留窗内的终态记录
        self._records: dict[str, CommandRecord] = {}
        # 终态完成顺序（FIFO 淘汰依据）；仅含仍在 _records 中的终态指令
        self._finish_order: deque[str] = deque()
        # 已淘汰指令的有界墓碑：command_id -> 淘汰前的终态快照
        self._retired: dict[str, CommandStatus] = {}
        self._retired_order: deque[str] = deque()
        # 提交对账计数
        self._submitted = 0
        self._accepted = 0
        self._duplicates = 0
        self._rejected: dict[str, int] = {}
        self._terminal: dict[str, int] = {s.value: 0 for s in TERMINAL_STATES}

    # ---- 提交对账计数（每单恰好一次归类） ----
    def count_submission(self) -> None:
        self._submitted += 1

    def count_accepted(self) -> None:
        self._accepted += 1

    def count_duplicate(self) -> None:
        self._duplicates += 1

    def count_rejection(self, reason: str) -> None:
        self._rejected[reason] = self._rejected.get(reason, 0) + 1

    # ---- 记录的建立与读取 ----
    def get(self, command_id: str) -> CommandRecord | None:
        return self._records.get(command_id)

    def create_queued(
        self, command_id: str, device_id: str, name: str
    ) -> CommandRecord:
        """登记一条 QUEUED 记录（调用方已确保 id 不与活跃/墓碑冲突）。"""
        record = CommandRecord(
            command_id=command_id, device_id=device_id, name=name
        )
        self._records[command_id] = record
        self.count_accepted()
        logger.info(
            "command accepted command_id=%s device=%s name=%s state=%s",
            command_id, device_id, name, CommandState.QUEUED.value,
        )
        return record

    def create_rejected(
        self,
        command_id: str,
        device_id: str,
        name: str,
        reason: ParseError,
    ) -> CommandRecord:
        """登记一条 REJECTED 终态记录（提交那一刻即被拒、从未入队/执行）。

        与 create_queued 的区别：不增加 accepted（拒绝计数由调用方完成），
        记录从建立起就是终态，立即进入终态 FIFO 容量治理（保留窗 + 墓碑环），
        因此被拒指令长期累积也不会无界增长。event 立即置位：该指令没有任何
        在途执行，等待方（含并发同号提交）无需阻塞即可拿到确定结论。
        """
        record = CommandRecord(
            command_id=command_id,
            device_id=device_id,
            name=name,
            state=CommandState.REJECTED,
            reason=reason,
        )
        self._records[command_id] = record
        self._terminal[CommandState.REJECTED.value] += 1
        self._finish_order.append(record.command_id)
        logger.info(
            "command terminal command_id=%s device=%s state=%s reason=%s",
            command_id, device_id, CommandState.REJECTED.value, reason.reason,
        )
        self._evict_if_needed()
        record.event.set()
        return record

    def retired_snapshot(self, command_id: str) -> CommandStatus | None:
        return self._retired.get(command_id)

    def status(self, command_id: str) -> CommandStatus | None:
        """查询：活跃/保留窗内 -> 终态或当前快照(retired=False)；
        墓碑窗内 -> 淘汰前终态快照(retired=True)；彻底不存在 -> None。"""
        record = self._records.get(command_id)
        if record is not None:
            return CommandStatus(
                command_id=record.command_id,
                state=record.state,
                device_id=record.device_id,
                name=record.name,
                response=record.response,
                reason=record.reason,
                retired=False,
            )
        return self._retired.get(command_id)

    def result_snapshot(self, command_id: str) -> CommandStatus:
        """提交方等待结束后取结论：记录可能已被淘汰，墓碑里必有快照。"""
        snapshot = self.status(command_id)
        if snapshot is None:
            # 理论不可达：终态记录在墓碑窗内至少保留 max_retired 条
            raise KeyError(f"command {command_id!r} vanished without a trace")
        return snapshot

    @staticmethod
    def snapshot_record(
        record: CommandRecord, retired: bool = False
    ) -> CommandStatus:
        """直接基于记录对象取快照：提交方持有本地引用，即使记录已被淘汰出
        注册表/墓碑窗，也能拿到自己那一单的确定结论。"""
        return CommandStatus(
            command_id=record.command_id,
            state=record.state,
            device_id=record.device_id,
            name=record.name,
            response=record.response,
            reason=record.reason,
            retired=retired,
        )

    # ---- 状态流转（只进不退） ----
    def mark_running(self, record: CommandRecord) -> None:
        if record.state is not CommandState.QUEUED:
            return
        record.state = CommandState.RUNNING
        logger.info(
            "command state-change command_id=%s device=%s -> %s",
            record.command_id, record.device_id, CommandState.RUNNING.value,
        )

    def finish_terminal(
        self,
        record: CommandRecord,
        state: CommandState,
        response: Any = None,
        reason: ParseError | None = None,
    ) -> None:
        """RUNNING -> SUCCEEDED/FAILED（终态）。"""
        assert state in (CommandState.SUCCEEDED, CommandState.FAILED)
        record.state = state
        record.response = response
        record.reason = reason
        self._terminal[state.value] += 1
        self._finish_order.append(record.command_id)
        logger.info(
            "command terminal command_id=%s device=%s state=%s reason=%s",
            record.command_id, record.device_id, state.value,
            reason.reason if reason else "-",
        )
        self._evict_if_needed()
        record.event.set()

    def mark_cancelled(self, record: CommandRecord) -> bool:
        """QUEUED -> CANCELLED。

        关键：此刻指令仍占着队列位置、worker 尚未轮到它。记录绝不能在此刻进入
        终态淘汰序列（否则可能在 worker 取走前被当垃圾清掉）。因此只翻转状态、
        唤醒提交方并让其计入对账；由 worker 真正跳过它时调用 retire_cancelled
        再进入 FIFO 终态序列。返回是否为首次取消（False 表示重复取消，幂等）。
        """
        if record.state is not CommandState.QUEUED:
            return False
        record.state = CommandState.CANCELLED
        self._terminal[CommandState.CANCELLED.value] += 1
        logger.info(
            "command terminal command_id=%s device=%s state=%s "
            "(cancelled before execution; still holding queue slot)",
            record.command_id, record.device_id, CommandState.CANCELLED.value,
        )
        record.event.set()
        return True

    def retire_cancelled(self, record: CommandRecord) -> None:
        """worker 轮过一条已取消指令：此刻它才离开队列，正式进入终态淘汰序列。"""
        if record.state is not CommandState.CANCELLED:
            return
        self._finish_order.append(record.command_id)
        self._evict_if_needed()

    def cancel(self, command_id: str) -> tuple[CommandStatus, bool] | None:
        """取消语义（返回 (终态快照, duplicate)；未知 id 返回 None）。

        * QUEUED                 -> CANCELLED，首次取消 duplicate=False；
        * 已 CANCELLED（含墓碑） -> 幂等，duplicate=True；
        * RUNNING/其它终态(含墓碑) -> 拒绝由调用方按 NOT_CANCELLABLE 处理，
          本方法把当前快照原样返回（duplicate=False）供调用方判定。
        """
        record = self._records.get(command_id)
        if record is None:
            tomb = self._retired.get(command_id)
            if tomb is None:
                return None
            if tomb.state is CommandState.CANCELLED:
                return tomb, True
            return tomb, False

        if record.state is CommandState.QUEUED:
            first = self.mark_cancelled(record)
            return self.status(command_id), not first  # type: ignore[return-value]

        if record.state is CommandState.CANCELLED:
            return self.status(command_id), True  # type: ignore[return-value]
        return self.status(command_id), False  # type: ignore[return-value]

    # ---- 容量治理 ----
    def _evict_if_needed(self) -> None:
        """终态记录超过 max_history：最早完成的终态记录降级为墓碑。

        非终态记录不在 _finish_order 中，因此绝不可能被清理。
        """
        while len(self._finish_order) > self._max_history:
            oldest = self._finish_order.popleft()
            record = self._records.pop(oldest, None)
            if record is None:
                continue  # 已被其它路径移除（防御性）
            snapshot = CommandStatus(
                command_id=record.command_id,
                state=record.state,
                device_id=record.device_id,
                name=record.name,
                response=record.response,
                reason=record.reason,
                retired=True,
            )
            self._retired[oldest] = snapshot
            self._retired_order.append(oldest)
            logger.info(
                "command history evicted command_id=%s state=%s "
                "history_limit=%d",
                oldest, record.state.value, self._max_history,
            )
            while len(self._retired_order) > self._max_retired:
                gone = self._retired_order.popleft()
                self._retired.pop(gone, None)
                logger.info(
                    "command tombstone expired command_id=%s retired_limit=%d",
                    gone, self._max_retired,
                )

    # ---- 对账 ----
    def metrics_snapshot(self) -> CommandMetrics:
        active = 0
        for record in self._records.values():
            if record.state not in TERMINAL_STATES:
                active += 1
        return CommandMetrics(
            submitted=self._submitted,
            accepted=self._accepted,
            duplicates=self._duplicates,
            rejected=dict(self._rejected),
            terminal=dict(self._terminal),
            active=active,
            history=len(self._finish_order),
            retired=len(self._retired),
        )
