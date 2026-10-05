"""下行指令：鉴权、串行执行、有界积压、幂等去重、状态查询、取消与容量治理。"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from queue import Full, Queue
from typing import Any, Callable

from gateway.commandstore import (
    BoundedCommandStore,
    CommandMetrics,
    CommandState,
    CommandStatus,
)
from gateway.protocol import (
    R_COMMAND_DUPLICATE,
    R_COMMAND_HANDLER_ERROR,
    R_COMMAND_NOT_CANCELLABLE,
    R_COMMAND_NOT_FOUND,
    R_COMMAND_QUEUE_FULL,
    R_COMMAND_RETIRED,
    R_COMMAND_UNAUTHORIZED,
    R_COMMAND_UNSUPPORTED,
    R_DEVICE_UNKNOWN,
    ParseError,
)

logger = logging.getLogger("gateway.commands")


@dataclass(frozen=True)
class Command:
    device_id: str
    command_id: str
    name: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass
class CommandResult:
    command_id: str
    status: str  # "ok" | "rejected" | "cancelled"
    device_id: str = ""
    response: Any = None
    reason: ParseError | None = None
    duplicate: bool = False  # True: 该 command_id 此前已处理过，本次未重复执行/取消


# 指令处理器：name -> handler(device_id, args) -> response
HandlerTable = dict[str, Callable[[str, dict[str, Any]], Any]]


@dataclass
class _DeviceState:
    queue: "Queue[Command | None]"
    thread: threading.Thread
    # 严格提交顺序由 Queue 本身保证；取消通过共享状态机标记，出队时跳过执行。


def _reject(command_id: str, reason: ParseError, device_id: str = "") -> CommandResult:
    return CommandResult(
        command_id=command_id, status="rejected", device_id=device_id, reason=reason
    )


def _status_to_result(status: CommandStatus, duplicate: bool = False) -> CommandResult:
    if status.state is CommandState.SUCCEEDED:
        return CommandResult(
            command_id=status.command_id,
            status="ok",
            device_id=status.device_id,
            response=status.response,
            duplicate=duplicate,
        )
    if status.state is CommandState.CANCELLED:
        return CommandResult(
            command_id=status.command_id,
            status="cancelled",
            device_id=status.device_id,
            duplicate=duplicate,
        )
    return CommandResult(
        command_id=status.command_id,
        status="rejected",
        device_id=status.device_id,
        reason=status.reason,
        duplicate=duplicate,
    )


class CommandExecutor:
    """下行指令执行器。

    并发语义：
      * 每个设备一把串行队列 + 一个工作线程 => 同设备指令严格按提交顺序生效；
      * command_id 全局幂等 => 并发提交相同 command_id 只执行一次，无状态回退；
      * 队列长度有界 => 积压超限返回 COMMAND_QUEUE_FULL，不做无界堆积；
      * 提交即拒（设备未知/未授权/积压超限）的指令同样落 REJECTED 终态记录：
        凭 command_id 可查到明确、可区分的“已拒绝”结论，与“从未提交”的
        COMMAND_NOT_FOUND 截然不同，且与成功/失败/取消一样受终态容量治理；
      * 指令状态进入 BoundedCommandStore：可凭 command_id 查询、可取消尚未
        开始执行者、终态按 FIFO 有界保留并以墓碑防止二次执行。
    """

    def __init__(
        self,
        authorizations: dict[str, frozenset[str]],
        handlers: HandlerTable,
        max_queue: int = 16,
        max_history: int = 256,
        max_retired: int = 4096,
    ) -> None:
        self._auth = authorizations
        self._handlers = handlers
        self._max_queue = max_queue
        self._devices: dict[str, _DeviceState] = {}
        self._lock = threading.Lock()
        self._store = BoundedCommandStore(
            max_history=max_history, max_retired=max_retired
        )

    # ------------------------------------------------------------------
    def _submission_rejection_locked(
        self, command: Command, reason: ParseError
    ) -> CommandResult:
        """“提交即拒”的统一落账（必须在 self._lock 内调用）。

        * 该 command_id 已有结论（记录或墓碑）-> 绝不覆盖他人记录、绝不
          重复执行：同设备复用既有结论（duplicate 幂等）；跨设备
          COMMAND_DUPLICATE；已淘汰墓碑 COMMAND_RETIRED —— 同一指令号
          无论查多少次、再提交多少次，结论稳定一致；
        * 否则登记一条新的 REJECTED 终态记录（可凭号查询、受终态容量
          治理），并计入对应拒绝原因，对账口径不变。
        """
        command_id = command.command_id
        device_id = command.device_id
        tomb = self._store.retired_snapshot(command_id)
        if tomb is not None:
            self._store.count_rejection(R_COMMAND_RETIRED)
            logger.info(
                "command reject command_id=%s input=device=%s name=%s reason=%s "
                "prior_state=%s",
                command_id, device_id, command.name, R_COMMAND_RETIRED,
                tomb.state.value,
            )
            return _reject(
                command_id,
                ParseError(
                    R_COMMAND_RETIRED,
                    f"command_id {command_id!r} already concluded "
                    f"({tomb.state.value}) and retired from the retention "
                    "window",
                ),
                device_id=tomb.device_id,
            )
        existing = self._store.get(command_id)
        if existing is not None:
            if existing.device_id != device_id:
                self._store.count_rejection(R_COMMAND_DUPLICATE)
                logger.info(
                    "command reject command_id=%s input=device=%s name=%s "
                    "reason=%s owner=%s",
                    command_id, device_id, command.name,
                    R_COMMAND_DUPLICATE, existing.device_id,
                )
                return _reject(
                    command_id,
                    ParseError(
                        R_COMMAND_DUPLICATE,
                        "command_id already used by another device",
                    ),
                    device_id=existing.device_id,
                )
            # 同设备同号：复用既有结论（典型即此前的 REJECTED 记录），幂等
            self._store.count_duplicate()
            logger.info(
                "command duplicate-of-rejection command_id=%s input=device=%s "
                "name=%s prior_state=%s",
                command_id, device_id, command.name, existing.state.value,
            )
            return _status_to_result(
                self._store.snapshot_record(existing), duplicate=True
            )
        self._store.count_rejection(reason.reason)
        self._store.create_rejected(command_id, device_id, command.name, reason)
        logger.info(
            "command reject command_id=%s input=device=%s name=%s reason=%s",
            command_id, device_id, command.name, reason.reason,
        )
        return _reject(command_id, reason, device_id=device_id)

    # ------------------------------------------------------------------
    def _get_or_create_device_locked(self, device_id: str) -> _DeviceState:
        state = self._devices.get(device_id)
        if state is None:
            queue: Queue[Command | None] = Queue(maxsize=self._max_queue)
            state = _DeviceState(queue=queue, thread=None)  # type: ignore[arg-type]
            thread = threading.Thread(
                target=self._worker_loop,
                args=(device_id, state),
                name=f"cmd-{device_id}",
                daemon=True,
            )
            state.thread = thread
            self._devices[device_id] = state
            thread.start()
        return state

    def _worker_loop(self, device_id: str, state: _DeviceState) -> None:
        """单设备单线程：保证该设备指令串行生效、提交顺序即生效顺序。

        被取消的指令保留其队列位置（从而不打乱其它指令顺序），仅在轮到它时
        跳过执行；状态 CANCELLED 由取消路径写入，这里绝不覆盖。
        """
        while True:
            command = state.queue.get()
            try:
                if command is None:
                    return
                with self._lock:
                    record = self._store.get(command.command_id)
                    if record is None:
                        # 防御性：记录不应在出队前消失（非终态不淘汰）
                        logger.error(
                            "worker dequeued command without record command_id=%s",
                            command.command_id,
                        )
                        continue
                    if record.state is not CommandState.QUEUED:
                        if record.state is CommandState.CANCELLED:
                            # 轮到它了：此刻才真正离开队列，允许进入终态淘汰序列
                            self._store.retire_cancelled(record)
                        else:
                            logger.error(
                                "worker dequeued command in unexpected state "
                                "command_id=%s state=%s",
                                command.command_id, record.state.value,
                            )
                        # 已取消：跳过执行，绝不回退状态、绝不二次执行
                        logger.info(
                            "worker skips command command_id=%s state=%s",
                            command.command_id, record.state.value,
                        )
                        continue
                    # RUNNING 标记在锁内完成；处理器在锁外执行（可能耗时，
                    # 且提交方的入队判定不应被慢处理器阻塞）
                    self._store.mark_running(record)
                outcome = self._execute(command)
                with self._lock:
                    if outcome.status == "ok":
                        self._store.finish_terminal(
                            record, CommandState.SUCCEEDED, response=outcome.response
                        )
                    else:
                        assert outcome.reason is not None
                        self._store.finish_terminal(
                            record, CommandState.FAILED, reason=outcome.reason
                        )
            finally:
                state.queue.task_done()

    def _execute(self, command: Command) -> CommandResult:
        """鉴权已在提交时完成；这里只做分发执行与错误区分。"""
        handler = self._handlers.get(command.name)
        if handler is None:
            return _reject(
                command.command_id,
                ParseError(
                    R_COMMAND_UNSUPPORTED,
                    f"command {command.name!r} has no handler",
                ),
                device_id=command.device_id,
            )
        try:
            response = handler(command.device_id, dict(command.args))
        except Exception as exc:  # 业务处理器异常不得破坏串行循环
            return _reject(
                command.command_id,
                ParseError(R_COMMAND_HANDLER_ERROR, f"{type(exc).__name__}: {exc}"),
                device_id=command.device_id,
            )
        return CommandResult(
            command.command_id,
            status="ok",
            device_id=command.device_id,
            response=response,
        )

    # ------------------------------------------------------------------
    def submit(self, command: Command) -> CommandResult:
        """提交并阻塞至该指令被串行执行完毕（或被取消）。

        每个提交在锁内被归类为 accepted / duplicate / rejected 之一，对账恒平。
        提交即拒（设备未知/未授权/积压超限）也会落 REJECTED 终态记录，
        事后凭 command_id 可查到与“从未提交”明确可区分的结论。
        """
        device_id = command.device_id
        command_id = command.command_id

        # 预校验（设备/授权）：被拒指令也登记 REJECTED 终态记录，使事后可凭
        # command_id 查到明确、可区分的“已拒绝”结论（与“从未提交”截然不同）；
        # 对账上仍只计入 submitted 与对应 rejected 原因，口径不变。
        with self._lock:
            self._store.count_submission()
            allowed = self._auth.get(device_id)
            if allowed is None:
                return self._submission_rejection_locked(
                    command,
                    ParseError(
                        R_DEVICE_UNKNOWN,
                        f"device {device_id!r} is not registered",
                    ),
                )
            if command.name not in allowed:
                return self._submission_rejection_locked(
                    command,
                    ParseError(
                        R_COMMAND_UNAUTHORIZED,
                        f"device {device_id!r} is not authorized for "
                        f"{command.name!r}",
                    ),
                )

        duplicate = False
        record = None
        with self._lock:
            # 墓碑窗内复用：明确、可区分地拒绝，防止同一指令二次执行
            tomb = self._store.retired_snapshot(command_id)
            if tomb is not None:
                self._store.count_rejection(R_COMMAND_RETIRED)
                logger.info(
                    "command reject command_id=%s input=device=%s name=%s reason=%s "
                    "prior_state=%s",
                    command_id, device_id, command.name, R_COMMAND_RETIRED,
                    tomb.state.value,
                )
                return _reject(
                    command_id,
                    ParseError(
                        R_COMMAND_RETIRED,
                        f"command_id {command_id!r} already completed "
                        f"({tomb.state.value}) and retired from the retention "
                        "window",
                    ),
                    device_id=tomb.device_id,
                )

            record = self._store.get(command_id)
            if record is not None:
                if record.device_id != device_id:
                    # 跨设备复用 command_id：拒绝，且不重复执行
                    self._store.count_rejection(R_COMMAND_DUPLICATE)
                    logger.info(
                        "command reject command_id=%s input=device=%s name=%s "
                        "reason=%s owner=%s",
                        command_id, device_id, command.name,
                        R_COMMAND_DUPLICATE, record.device_id,
                    )
                    return _reject(
                        command_id,
                        ParseError(
                            R_COMMAND_DUPLICATE,
                            "command_id already used by another device",
                        ),
                        device_id=record.device_id,
                    )
                if record.state is CommandState.REJECTED:
                    # 该 id 此前提交即被拒：复用同一拒绝结论（幂等），
                    # 绝不因队列已有空位/授权变更而改判或补执行
                    self._store.count_duplicate()
                    logger.info(
                        "command duplicate-of-rejection command_id=%s "
                        "input=device=%s name=%s reason=%s",
                        command_id, device_id, command.name,
                        record.reason.reason if record.reason else "-",
                    )
                    return _status_to_result(
                        self._store.snapshot_record(record), duplicate=True
                    )
                # 并发/重复提交：等待首个提交的结论，本调用不重复执行
                self._store.count_duplicate()
                event = record.event
                duplicate = True
            else:
                state = self._get_or_create_device_locked(device_id)
                try:
                    # Queue 自身有界：max_queue 个排队槽 + 1 个已出队在途槽，
                    # 即有效积压口径“在途 + 排队 <= max_queue + 1”，超限即拒绝。
                    state.queue.put_nowait(command)
                except Full:
                    # 积压超限被拒同样登记 REJECTED 终态：事后凭号可查、
                    # 与从未提交可区分；受同一套终态容量治理，不无界增长
                    return self._submission_rejection_locked(
                        command,
                        ParseError(
                            R_COMMAND_QUEUE_FULL,
                            f"backlog limit {self._max_queue} reached for "
                            f"{device_id!r}",
                        ),
                    )
                # 入队成功才建立状态（QUEUED）并计入 accepted
                record = self._store.create_queued(command_id, device_id, command.name)
                event = record.event

        event.wait()
        # 用本地持有的记录对象取结论：即使该 id 已被淘汰出保留窗/墓碑窗，
        # 提交方仍能拿到自己这一单的确定结论（不依赖再次按 id 查表）。
        assert record is not None
        with self._lock:
            final = self._store.snapshot_record(record)
        logger.info(
            "command settled command_id=%s device=%s state=%s duplicate=%s",
            command_id, final.device_id, final.state.value, duplicate,
        )
        return _status_to_result(final, duplicate=duplicate)

    # ------------------------------------------------------------------
    # ---- 指令状态查询 / 取消 / 对账 ----
    def query_status(self, command_id: str) -> CommandStatus | ParseError:
        """凭指令号查询：同一 id 反复查询结论稳定一致；未提交过 -> NOT_FOUND。

        提交即拒的指令返回 REJECTED 终态快照（含具体拒绝原因），与
        COMMAND_NOT_FOUND（从未提交/已被彻底遗忘）明确可区分。
        """
        with self._lock:
            status = self._store.status(command_id)
        if status is None:
            logger.info(
                "command query command_id=%s verdict=%s",
                command_id, R_COMMAND_NOT_FOUND,
            )
            return ParseError(
                R_COMMAND_NOT_FOUND,
                f"command_id {command_id!r} has never been submitted",
            )
        logger.info(
            "command query command_id=%s verdict=found state=%s retired=%s",
            command_id, status.state.value, status.retired,
        )
        return status

    def cancel(self, command_id: str) -> CommandResult:
        """取消尚未开始执行的指令。

        * QUEUED -> CANCELLED（成功）；重复取消幂等（duplicate=True）；
        * RUNNING / SUCCEEDED / FAILED（含已淘汰墓碑）-> COMMAND_NOT_CANCELLABLE；
        * 从未提交 -> COMMAND_NOT_FOUND；
        * 取消仅做标记、保留队列位置，不改变同设备其它指令的先后。
        """
        with self._lock:
            outcome = self._store.cancel(command_id)
            if outcome is None:
                logger.info(
                    "command cancel command_id=%s verdict=%s",
                    command_id, R_COMMAND_NOT_FOUND,
                )
                return _reject(
                    command_id,
                    ParseError(
                        R_COMMAND_NOT_FOUND,
                        f"command_id {command_id!r} has never been submitted",
                    ),
                )
            status, was_duplicate = outcome
            if status.state is CommandState.CANCELLED:
                logger.info(
                    "command cancel command_id=%s verdict=cancelled duplicate=%s",
                    command_id, was_duplicate,
                )
                return _status_to_result(status, duplicate=was_duplicate)
            # RUNNING 或已成功/失败（含墓碑）：明确拒绝并给出原因
            logger.info(
                "command cancel command_id=%s verdict=%s state=%s retired=%s",
                command_id, R_COMMAND_NOT_CANCELLABLE,
                status.state.value, status.retired,
            )
            return _reject(
                command_id,
                ParseError(
                    R_COMMAND_NOT_CANCELLABLE,
                    f"command is {status.state.value}"
                    + (" (retired)" if status.retired else "")
                    + ", only queued commands can be cancelled",
                ),
                device_id=status.device_id,
            )

    def metrics(self) -> CommandMetrics:
        with self._lock:
            return self._store.metrics_snapshot()

    def shutdown(self) -> None:
        with self._lock:
            states = list(self._devices.values())
            for state in states:
                state.queue.put(None)
        for state in states:
            state.thread.join(timeout=2)
