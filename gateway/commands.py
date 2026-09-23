"""下行指令通道：鉴权、按设备串行执行、请求去重与有界积压。

语义：
- 设备未知 / 指令越权：以不同原因立即拒绝
  （``device_unknown`` / ``unauthorized_command``），不入队。
- 同一 (device, request_id) 重复提交：不重复执行，等待并返回首次执行
  结果，同时标记 ``duplicate=True``。
- 同一设备的指令严格按提交顺序**串行生效**：每个设备只有一个 worker，
  逐条 await 执行，因此并发提交不会出现重复执行或状态回退。
- 积压（排队中 + 执行中）达到 ``max_pending`` 时以
  ``command_queue_full`` 立即拒绝，不做无界堆积。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .errors import RejectCode
from .protocol import CommandRequest, CommandResult
from .registry import DeviceRegistry

CommandHandler = Callable[[CommandRequest], Awaitable[dict]]


async def _default_handler(req: CommandRequest) -> dict:
    """默认执行器：记录指令名与参数，返回成功状态。"""
    return {"ok": True, "command": req.command, "args": req.args}


class _PendingCommand:
    __slots__ = ("req", "future")

    def __init__(self, req: CommandRequest, future: asyncio.Future) -> None:
        self.req = req
        self.future = future


class CommandBus:
    def __init__(
        self,
        registry: DeviceRegistry,
        *,
        max_pending: int = 16,
        handler: CommandHandler | None = None,
    ) -> None:
        self._registry = registry
        self._max_pending = max_pending
        self._handler: CommandHandler = handler or _default_handler
        self._queues: dict[str, asyncio.Queue[_PendingCommand | None]] = {}
        self._workers: dict[str, asyncio.Task] = {}
        self._inflight: dict[str, dict[str, asyncio.Future]] = {}
        self._finished: dict[str, dict[str, CommandResult]] = {}
        self._bus_lock = asyncio.Lock()

    async def _ensure_worker(self, device_id: str) -> asyncio.Queue:
        async with self._bus_lock:
            queue = self._queues.get(device_id)
            if queue is None:
                queue = asyncio.Queue(maxsize=self._max_pending)
                self._queues[device_id] = queue
                self._inflight[device_id] = {}
                self._finished[device_id] = {}
                self._workers[device_id] = asyncio.create_task(
                    self._worker(device_id, queue),
                    name=f"command-worker-{device_id}",
                )
            return queue

    async def _worker(self, device_id: str, queue: asyncio.Queue) -> None:
        inflight = self._inflight[device_id]
        while True:
            item = await queue.get()
            try:
                if item is None:  # 关闭信号
                    return
                try:
                    state = await self._handler(item.req)
                    result = CommandResult(
                        accepted=True,
                        request_id=item.req.request_id,
                        executed=True,
                        state=state,
                    )
                except Exception as exc:  # 执行器异常不等于重复执行
                    result = CommandResult(
                        accepted=False,
                        request_id=item.req.request_id,
                        code=RejectCode.INTERNAL_ERROR,
                        reason=f"指令执行异常: {exc}",
                    )
                if not item.future.done():
                    item.future.set_result(result)
                # 缓存最终结果，使后续同 request_id 提交可识别为重复，
                # 且永远不再第二次执行。
                self._finished[device_id][item.req.request_id] = result
            finally:
                inflight.pop(item.req.request_id, None) if item else None
                queue.task_done()

    async def submit(self, req: CommandRequest) -> CommandResult:
        """提交一条指令：先鉴权/去重，再入有界队列等待串行执行。"""
        allowed, code = self._registry.authorize(req.device_id, req.command)
        if not allowed:
            return CommandResult(
                accepted=False,
                request_id=req.request_id,
                code=code,
                reason=(
                    f"设备 {req.device_id} 未注册"
                    if code is RejectCode.DEVICE_UNKNOWN
                    else f"设备 {req.device_id} 未被授权指令 {req.command!r}"
                ),
            )

        queue = await self._ensure_worker(req.device_id)
        inflight = self._inflight[req.device_id]
        finished = self._finished[req.device_id]
        duplicate = False
        first: CommandResult | None = None
        async with self._bus_lock:
            prior = finished.get(req.request_id)
            if prior is not None:
                # 已完成过的请求：直接返回首次结果，绝不二次执行。
                first = prior
                duplicate = True
            else:
                existing = inflight.get(req.request_id)
                if existing is not None:
                    duplicate = True
                    future = existing
                else:
                    if queue.qsize() >= self._max_pending:
                        return CommandResult(
                            accepted=False,
                            request_id=req.request_id,
                            code=RejectCode.COMMAND_QUEUE_FULL,
                            reason=f"设备 {req.device_id} 指令积压已达上限 {self._max_pending}",
                        )
                    future: asyncio.Future = asyncio.get_running_loop().create_future()
                    inflight[req.request_id] = future
                    item = _PendingCommand(req, future)
                    queue.put_nowait(item)

        if duplicate and first is not None:
            return CommandResult(
                accepted=first.accepted,
                request_id=first.request_id,
                executed=first.executed,
                state=first.state,
                duplicate=True,
                code=first.code,
                reason=first.reason,
            )
        first = await future
        if duplicate:
            return CommandResult(
                accepted=first.accepted,
                request_id=first.request_id,
                executed=first.executed,
                state=first.state,
                duplicate=True,
                code=first.code,
                reason=first.reason,
            )
        return first

    async def shutdown(self) -> None:
        """停止所有设备 worker（排空中的指令按可关闭语义处理）。"""
        async with self._bus_lock:
            for queue in self._queues.values():
                queue.put_nowait(None)
            tasks = list(self._workers.values())
            self._queues.clear()
            self._workers.clear()
            self._inflight.clear()
            self._finished.clear()
        for task in tasks:
            await task
