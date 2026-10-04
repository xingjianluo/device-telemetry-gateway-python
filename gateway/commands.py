"""下行指令：鉴权、串行执行、有界积压、幂等去重、状态查询与撤销。"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from queue import Full, Queue
from typing import Any, Callable

from gateway.protocol import (
    R_COMMAND_CANCELED,
    R_COMMAND_DUPLICATE,
    R_COMMAND_HANDLER_ERROR,
    R_COMMAND_UNAUTHORIZED,
    R_COMMAND_UNSUPPORTED,
    R_DEVICE_UNKNOWN,
    ParseError,
)
from gateway.registry import (
    CancelResult,
    CommandRegistry,
    CommandStatus,
    CommandStatusView,
)


@dataclass(frozen=True)
class Command:
    device_id: str
    command_id: str
    name: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass
class CommandResult:
    command_id: str
    # "ok" | "rejected" | "canceled"
    status: str
    response: Any = None
    reason: ParseError | None = None
    duplicate: bool = False  # True: 该 command_id 此前已有结论，本次复用，未重复执行


# 指令处理器：name -> handler(device_id, args) -> response
HandlerTable = dict[str, Callable[[str, dict[str, Any]], Any]]


@dataclass
class _DeviceState:
    queue: "Queue[Command | None]"
    thread: threading.Thread


def _result_from_view(view: CommandStatusView) -> CommandResult:
    """把登记册视图映射为提交返回体（查询用视图本身，语义更丰富）。"""
    if view.status == CommandStatus.SUCCEEDED.value:
        return CommandResult(
            view.command_id, status="ok",
            response=view.response, duplicate=view.duplicate,
        )
    if view.status == CommandStatus.CANCELED.value:
        return CommandResult(
            view.command_id, status="canceled",
            reason=view.reason or ParseError(
                R_COMMAND_CANCELED, "command was canceled before execution",
            ),
            duplicate=view.duplicate,
        )
    return CommandResult(
        view.command_id, status="rejected",
        reason=view.reason, duplicate=view.duplicate,
    )


class CommandExecutor:
    """下行指令执行器。

    并发与生命周期语义：
      * 每个设备一把串行队列 + 一个工作线程 => 同设备指令严格按提交顺序生效，
        撤销排队中的指令不会打乱其它指令的先后；
      * command_id 全局幂等 => 并发提交相同 command_id 只执行一次；
        每条指令都有 PENDING->EXECUTING->(SUCCEEDED|REJECTED|CANCELED)
        的单向状态机，不存在回退边，不可能"既被取消又被执行"；
      * 队列长度有界 => 积压超限 COMMAND_QUEUE_FULL；
      * 终态由有界 CommandRegistry 保存 => 可凭 command_id 事后查询、可对账。
    """

    def __init__(
        self,
        authorizations: dict[str, frozenset[str]],
        handlers: HandlerTable,
        max_queue: int = 16,
        max_status_records: int = 1024,
    ) -> None:
        self._auth = authorizations
        self._handlers = handlers
        self._max_queue = max_queue
        self._devices: dict[str, _DeviceState] = {}
        self._lock = threading.Lock()  # 仅保护设备表创建
        self._registry = CommandRegistry(max_records=max_status_records)

    # ------------------------------------------------------------------
    def _get_or_create_device(self, device_id: str) -> _DeviceState:
        with self._lock:
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
        """单设备单线程：保证该设备指令串行生效、提交顺序即生效顺序。"""
        while True:
            command = state.queue.get()
            try:
                if command is None:
                    return
                # 状态机 CAS：只有仍在 PENDING（排队中、未撤销）才真正执行。
                # 已被撤销的指令直接跳过——既不调用处理器，也不影响后续指令顺序。
                if not self._registry.begin_execution(command.command_id):
                    continue
                ok, payload = self._execute(command)
                if ok:
                    self._registry.finish_succeeded(command.command_id, payload)
                else:
                    self._registry.finish_exec_rejected(command.command_id, payload)
            finally:
                state.queue.task_done()

    def _execute(self, command: Command) -> tuple[bool, Any]:
        """鉴权与状态判定均已在登记册完成；这里只做分发执行与错误区分。"""
        handler = self._handlers.get(command.name)
        if handler is None:
            return False, ParseError(
                R_COMMAND_UNSUPPORTED,
                f"command {command.name!r} has no handler",
            )
        try:
            response = handler(command.device_id, dict(command.args))
        except Exception as exc:  # 业务处理器异常不得破坏串行循环
            return False, ParseError(
                R_COMMAND_HANDLER_ERROR, f"{type(exc).__name__}: {exc}"
            )
        return True, response

    # ------------------------------------------------------------------
    def _auth_error(self, device_id: str, name: str) -> ParseError | None:
        allowed = self._auth.get(device_id)
        if allowed is None:
            return ParseError(
                R_DEVICE_UNKNOWN, f"device {device_id!r} is not registered",
            )
        if name not in allowed:
            return ParseError(
                R_COMMAND_UNAUTHORIZED,
                f"device {device_id!r} is not authorized for {name!r}",
            )
        return None

    def submit(self, command: Command) -> CommandResult:
        """提交一条指令并阻塞至其终态。终态可事后凭 command_id 查询。"""
        auth_error = self._auth_error(command.device_id, command.name)
        decision = self._registry.submit_decision(
            command.command_id, command.device_id, command.name, auth_error,
        )

        if decision.kind == "cross_device":
            # 保持上一轮语义：同号跨设备是明确可区分的拒绝
            return CommandResult(
                command.command_id, status="rejected",
                reason=ParseError(
                    R_COMMAND_DUPLICATE,
                    "command_id already used by another device",
                ),
            )
        if decision.kind == "new_rejected" or decision.kind == "limit":
            assert decision.view is not None
            return _result_from_view(decision.view)
        if decision.kind == "reuse_tomb":
            assert decision.view is not None
            return _result_from_view(decision.view)
        if decision.kind == "reuse_live":
            assert decision.record is not None
            # 并发/重复提交：阻塞等待首个提交的终态，本调用绝不重复执行
            view = self._registry.wait_terminal_view(decision.record, duplicate=True)
            return _result_from_view(view)

        # kind == "new_pending"：鉴权通过、已入册，放入该设备 FIFO
        assert decision.record is not None
        state = self._get_or_create_device(command.device_id)
        try:
            state.queue.put_nowait(command)
        except Full:
            # 与撤销竞争时以登记册里的终态为准（可能已先被撤销）
            view = self._registry.reject_queue_full(
                command.command_id, self._max_queue,
            )
            return _result_from_view(view)
        view = self._registry.wait_terminal_view(decision.record, duplicate=False)
        return _result_from_view(view)

    # ------------------------------------------------------------------
    def query(self, command_id: str) -> CommandStatusView | None:
        """凭指令号查询终态；从未提交过（或已超保留窗口）返回 None。

        结论稳定：同一 command_id 反复查询内容一致；成功/被拒/被取消/无此号
        四类答复相互可区分。
        """
        return self._registry.query(command_id)

    def cancel(self, command_id: str) -> CancelResult:
        """撤销一条尚未开始执行的指令。

        排队中(PENDING) -> 撤销成功；重复撤销 -> 幂等；已开始执行/已完成 ->
        COMMAND_CANCEL_TOO_LATE；未知指令号 -> COMMAND_NOT_FOUND。
        """
        return self._registry.cancel(command_id)

    def metrics(self) -> dict[str, int]:
        """容量与受理对账计数。"""
        return self._registry.metrics().to_dict()

    def check_invariants(self) -> None:
        """提交账/受理账/容量安全不变式断言（测试与巡检用）。"""
        self._registry.check_invariants()

    def shutdown(self) -> None:
        with self._lock:
            states = list(self._devices.values())
            for state in states:
                state.queue.put(None)
        for state in states:
            state.thread.join(timeout=2)
