# 下行指令：鉴权与并发语义

## 1. 模型

- `CommandRequest(device_id, command, request_id, args)`：一次下行指令。
- 设备可执行的指令集合在 `DeviceRegistry.register(device_id, allowed_commands)`
  中预先授权。
- 执行器是一个 async handler，默认实现只回显成功；业务方可注入自己的
  handler（例如真正驱动设备状态机）。

## 2. 鉴权（先于排队）

提交时立即判定，**不入队**：

1. 设备未注册 → `accepted=false, code=device_unknown`；
2. 指令不在授权集合 → `accepted=false, code=unauthorized_command`。

两者是**不同的拒绝码**，调用方可明确区分"设备不存在"与"越权"。

## 3. 并发控制

- **单设备单 worker**：每个设备一条 asyncio 队列 + 一个消费协程，指令被
  逐条 `await` 执行。因此同一设备的指令天然**串行生效**，执行顺序严格等于
  **提交顺序**，并发提交不会出现交错执行、重复执行或状态回退。
- **不同设备之间并行**：各自有独立 worker，互不阻塞。
- **请求去重**：以 `(device_id, request_id)` 为幂等键。
  - 前一条仍在排队/执行时并发提交相同 request_id：二者等待同一个结果，
    只有一条真正执行；后到者结果中 `duplicate=true`。
  - 前一条已执行完成后再提交相同 request_id：直接返回首次结果并标记
    `duplicate=true`，**永远不会第二次执行**。
- **有界积压**：每设备待处理（排队中 + 执行中）指令数达到 `max_pending`
  时，新的提交立即以 `code=command_queue_full` 拒绝，不做无界堆积。拒绝码
  与越权/未知设备可区分。

## 4. 结果字段

`CommandResult`：`accepted`、`executed`、`state`（执行器返回的状态）、
`duplicate`、`code`、`reason`。

- 正常：`accepted=true, executed=true, duplicate=false, state=...`
- 重放：`accepted=true, executed=true, duplicate=true, state=<首次结果>`
- 拒绝：`accepted=false, code=<可区分原因>, reason=<人类可读说明>`

## 5. 一致性断言（测试覆盖）

- 并发 N 条指令：接受数 + `command_queue_full` 拒绝数 == N（无静默丢失）；
- 实际生效顺序等于提交顺序（参数单调，无状态回退）；
- 每条指令恰好执行一次（无重复执行）；
- 相同 request_id 无论并发还是事后重放，handler 只被调用一次；
- 越权指令从不入队、从不执行。
