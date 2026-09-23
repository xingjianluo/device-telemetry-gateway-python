"""下行指令：鉴权、串行执行、有界积压、幂等去重。"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from queue import Full, Queue
from typing import Any, Callable

from gateway.protocol import (
    R_COMMAND_DUPLICATE,
    R_COMMAND_HANDLER_ERROR,
    R_COMMAND_QUEUE_FULL,
    R_COMMAND_UNAUTHORIZED,
    R_COMMAND_UNSUPPORTED,
    R_DEVICE_UNKNOWN,
    ParseError,
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
    status: str  # "ok" | "rejected"
    response: Any = None
    reason: ParseError | None = None
    duplicate: bool = False  # True: 该 command_id 此前已处理过，本次未重复执行


# 指令处理器：name -> handler(device_id, args) -> response
HandlerTable = dict[str, Callable[[str, dict[str, Any]], Any]]


@dataclass
class _DeviceState:
    queue: "Queue[Command | None]"
    thread: threading.Thread
    # 设备执行结果状态（由该设备的工作线程串行写入）
    applied: dict[str, CommandResult] = field(default_factory=dict)
    # 为保证严格提交顺序，pending 中的指令只登记 id；顺序由 Queue 本身保证


class CommandExecutor:
    """下行指令执行器。

    并发语义：
      * 每个设备一把串行队列 + 一个工作线程 => 同设备指令严格按提交顺序生效；
      * command_id 全局幂等 => 并发提交相同 command_id 只执行一次，无状态回退；
      * 队列长度有界 => 积压超限返回 COMMAND_QUEUE_FULL，不做无界堆积。
    """

    def __init__(
        self,
        authorizations: dict[str, frozenset[str]],
        handlers: HandlerTable,
        max_queue: int = 16,
    ) -> None:
        self._auth = authorizations
        self._handlers = handlers
        self._max_queue = max_queue
        self._devices: dict[str, _DeviceState] = {}
        self._lock = threading.Lock()
        # command_id -> (完成事件, 归属设备, 结果占位)
        self._inflight: dict[str, tuple[threading.Event, str, CommandResult | None]] = {}

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
        """单设备单线程：保证该设备指令串行生效、提交顺序即生效顺序。"""
        while True:
            command = state.queue.get()
            try:
                if command is None:
                    return
                result = self._execute(command)
                state.applied[command.command_id] = result
                with self._lock:
                    event, _owner, _placeholder = self._inflight[command.command_id]
                    self._inflight[command.command_id] = (event, device_id, result)
            finally:
                event.set()
                state.queue.task_done()

    def _execute(self, command: Command) -> CommandResult:
        """鉴权已在提交时完成；这里只做分发执行与错误区分。"""
        handler = self._handlers.get(command.name)
        if handler is None:
            return CommandResult(
                command.command_id,
                status="rejected",
                reason=ParseError(
                    R_COMMAND_UNSUPPORTED,
                    f"command {command.name!r} has no handler",
                ),
            )
        try:
            response = handler(command.device_id, dict(command.args))
        except Exception as exc:  # 业务处理器异常不得破坏串行循环
            return CommandResult(
                command.command_id,
                status="rejected",
                reason=ParseError(
                    R_COMMAND_HANDLER_ERROR, f"{type(exc).__name__}: {exc}"
                ),
            )
        return CommandResult(command.command_id, status="ok", response=response)

    # ------------------------------------------------------------------
    def submit(self, command: Command) -> CommandResult:
        """提交并阻塞至该指令被串行执行完毕。"""
        device_id = command.device_id
        allowed = self._auth.get(device_id)
        if allowed is None:
            return CommandResult(
                command.command_id,
                status="rejected",
                reason=ParseError(R_DEVICE_UNKNOWN, f"device {device_id!r} is not registered"),
            )
        if command.name not in allowed:
            return CommandResult(
                command.command_id,
                status="rejected",
                reason=ParseError(
                    R_COMMAND_UNAUTHORIZED,
                    f"device {device_id!r} is not authorized for {command.name!r}",
                ),
            )

        duplicate = False
        with self._lock:
            entry = self._inflight.get(command.command_id)
            if entry is not None:
                # 并发/重复提交：等待首个提交的执行结果，本调用不重复执行
                event, owner, _placeholder = entry
                if owner != device_id:
                    return CommandResult(
                        command.command_id,
                        status="rejected",
                        reason=ParseError(
                            R_COMMAND_DUPLICATE,
                            "command_id already used by another device",
                        ),
                    )
                duplicate = True
            else:
                event = threading.Event()
                self._inflight[command.command_id] = (event, device_id, None)
                state = self._get_or_create_device_locked(device_id)
                try:
                    state.queue.put_nowait(command)
                except Full:
                    del self._inflight[command.command_id]
                    return CommandResult(
                        command.command_id,
                        status="rejected",
                        reason=ParseError(
                            R_COMMAND_QUEUE_FULL,
                            f"backlog limit {self._max_queue} reached for {device_id!r}",
                        ),
                    )

        event.wait()
        _, _, result = self._inflight[command.command_id]
        assert result is not None
        if duplicate:
            result = CommandResult(
                result.command_id,
                status=result.status,
                response=result.response,
                reason=result.reason,
                duplicate=True,
            )
        return result

    def shutdown(self) -> None:
        with self._lock:
            states = list(self._devices.values())
            for state in states:
                state.queue.put(None)
        for state in states:
            state.thread.join(timeout=2)
