# 设备遥测接入网关（Python）

多台设备通过 **WebSocket 长连接** 上报遥测数据、通过 **HTTP** 接收下行指令。
网关在报文分片、乱序、重复、断线重连与高并发下发等不稳定条件下，仍提供
**一致、幂等、原因可区分** 的处理结果。

- 传输层：FastAPI（WebSocket `/ws/{device_id}`，HTTP `POST /commands`）
- 并发模型：每设备一把有界串行队列 + 独立工作线程；全局 `command_id` 幂等
- 无外部依赖，标准库 + FastAPI；单测使用 FastAPI TestClient

---

## 1. 运行

```bash
uv sync
uv run uvicorn main:app --port 8000
# 健康检查
curl http://127.0.0.1:8000/health
```

默认装配（见 `main.py`）：

| 设备 | 被授权指令 |
| --- | --- |
| `device-001` | `reboot`, `set_interval` |
| `device-002` | `set_interval` |

---

## 2. 报文约定

所有帧均为 **一个 JSON 对象的文本帧**，字段 `type` 标识类型。

### 2.1 遥测分片帧 `telemetry_fragment`

一条完整遥测报文可被切成 `total` 个分片，按 `index`（从 0 起）重组：

```json
{
  "type": "telemetry_fragment",
  "device_id": "device-001",
  "seq": 42,
  "index": 0,
  "total": 3,
  "version": "2",
  "payload_fragment": "{\"temperatu"
}
```

- 分片可 **乱序到达**：网关按 `(device_id, seq)` 缓冲，按 `index` 拼接。
- **重复分片**：同 `(seq,index)` 内容完全一致 → 幂等忽略；内容不一致
  → 拒绝 `DUPLICATE_FRAGMENT_CONFLICT`（防止错误重传污染数据）。
- 同一 `seq` 的 `total` / `version` 在各分片间必须一致，否则
  `FRAGMENT_HEADER_CONFLICT`。
- 缺片永不会被“猜全”。连接断开时未完成的报文以 `FRAGMENT_GAP` 明确拒绝，
  设备重连后依据续传游标重传。
- 分片头字段缺失 / 类型错误分别返回 `MISSING_FIELD` / `BAD_FIELD_TYPE`。

### 2.2 遥测 ACK `telemetry_ack`

```json
{ "type": "telemetry_ack", "seq": 42, "accepted": true,
  "duplicate": false, "complete": true, "record": { ... } }
```

- `complete=true`：本分片促成整条报文重组并落库。
- `duplicate=true`：该 `seq` 此前已确认，本次为幂等确认，**未产生新业务记录**。
- 失败时不带 `record`，改为 `error: {reason, detail}`。

### 2.3 续传帧 `resume`

建连即下发；设备也可随时发送 `{"type":"resume"}` 主动查询：

```json
{ "type": "resume", "device_id": "device-001", "known": true,
  "confirmed_seq": 41, "confirmed_seqs": [40, 41] }
```

设备据此 **只重传未确认 seq**；即使重传了已确认 seq，网关也只回
`duplicate=true`，不会重复落库（已确认数据不丢失、不重复）。

### 2.4 下行指令 `POST /commands`

```bash
curl -X POST http://127.0.0.1:8000/commands \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"device-001","command_id":"c-1","name":"reboot","args":{}}'
```

响应：

```json
{ "command_id": "c-1", "status": "ok", "duplicate": false,
  "response": { "rebooted": true } }
```

若设备在线，同一结果会以 `{"type":"command_result", ...}` 推送到其 WebSocket。

### 2.5 指令状态流转

每条被**接受**的指令在网关内部都有一个只进不退的状态机（与提交接口
原有 `status`/`duplicate` 字段并存、互不冲突）：

```
                 ┌────────── cancel (尚未开始执行) ──────────┐
                 ▼                                            │
   QUEUED ───────────────► RUNNING ───────────────► SUCCEEDED
     │                        │
     │ (worker 轮到它时发现已取消, 占位跳过)        └──────────► FAILED
     └──────────────────────────────────────────► CANCELLED
```

- `queued`：已入队，尚未开始执行（唯一可取消的状态）。
- `running`：设备工作线程已取出并开始执行，**不可取消**。
- `succeeded`：处理器正常返回（HTTP 提交侧 `status="ok"`）。
- `failed`：无处理器 / 处理器抛异常（提交侧 `status="rejected"`，
  原因 `COMMAND_UNSUPPORTED` / `COMMAND_HANDLER_ERROR`）。
- `cancelled`：开始执行前被取消（提交侧 `status="cancelled"`）。

后三者为**终态**，状态只进不退：不会出现“既取消又执行”，也不会从已完成
退回未完成。

### 2.6 查询指令 `GET /commands/{command_id}`

运维凭指令号随时查询，同一指令号反复查询结论稳定一致（返回不可变快照）：

```bash
curl http://127.0.0.1:8000/commands/c-1
# 200
# { "command_id":"c-1", "state":"succeeded", "device_id":"device-001",
#   "name":"reboot", "retired": false, "response": {"rebooted": true} }
```

- 执行中：`state` 为 `queued` / `running`；终态：`succeeded` / `failed`
  /`cancelled`，失败时带 `error.reason`，成功带 `response`。
- `retired=true`：该指令已按容量规则淘汰出保留窗，但仍在墓碑窗内，
  返回**淘汰前的终态**，结论明确、可区分。
- 指令号**从未提交**（或已超出墓碑窗被彻底遗忘）：`404` +
  `COMMAND_NOT_FOUND`，绝不与成功/失败含糊混淆。

### 2.7 取消指令 `POST /commands/{command_id}/cancel`

```bash
curl -X POST http://127.0.0.1:8000/commands/c-1/cancel
```

| 取消时状态 | 结果 |
| --- | --- |
| `queued`（尚未开始执行） | `200 {"status":"cancelled"}`，指令占位保留、轮到它时跳过，**不会执行** |
| 已 `cancelled`（含墓碑窗内） | `200` 幂等成功，`duplicate=true` |
| `running` / `succeeded` / `failed`（含墓碑） | `409 COMMAND_NOT_CANCELLABLE`，给出当前状态原因 |
| 从未提交 / 已彻底遗忘 | `404 COMMAND_NOT_FOUND` |

顺序保证：取消**只做状态标记，不从队列中摘除**，被取消指令仍占其队列位置，
因此同一台设备其它指令的相对先后顺序完全不变；worker 轮到它时才跳过，
并在这一刻才把它计入终态序列（避免它还在队列里时被容量清理误删）。
取消成功会唤醒所有等待该指令提交结果的调用方，得到 `cancelled` 结论；
若设备在线，还会收到一条 `command_result` 推送。

### 2.8 容量上限与清理规则

网关长期不重启，指令状态/取消记录只进不出会撑爆内存，因此有两级
**可配置、有界**的容量（构造 `CommandExecutor` 时注入，或用环境变量）：

| 参数 | 环境变量 | 默认 | 含义 |
| --- | --- | --- | --- |
| `max_queue` | `GATEWAY_MAX_QUEUE` | 16 | 每设备有界积压：在途 1 + 排队 `max_queue`，超限提交立即 `COMMAND_QUEUE_FULL` |
| `max_history` | `GATEWAY_MAX_HISTORY` | 256 | 终态指令结果保留条数，按完成先后 **FIFO 淘汰** |
| `max_retired` | `GATEWAY_MAX_RETIRED` | 4096 | 已淘汰指令的**墓碑**保留条数，环形有界，超出后最老记录被彻底遗忘 |

清理规则：

1. **非终态（`queued`/`running`）绝不参与淘汰** —— 清理不会把还没结束的
   指令当成不存在；它们也不在 FIFO 终态序列里。
2. 终态记录超过 `max_history` 时，最早完成的终态记录降级为**墓碑**
   （保留淘汰前的终态结论：状态/原因/响应）。
3. 墓碑超过 `max_retired` 时，最早的墓碑被彻底遗忘（之后查询回到
   `COMMAND_NOT_FOUND`）。
4. **防止重复执行**：记录或墓碑仍在保留窗内的 `command_id` 再次提交，一律
   以 `COMMAND_RETIRED` 拒绝（携带淘汰前终态），同一条指令不可能被执行两次。
5. 到顶该拒绝就拒绝：设备积压超限给 `COMMAND_QUEUE_FULL`；重用窗内 id 给
   `COMMAND_RETIRED`，调用方可据此重试或告警。

### 2.9 提交对账 `GET /metrics/commands`

每个提交请求在锁内被**恰好归类一次**，因此账目恒平：

```
submitted = accepted + duplicates + Σ rejected(reason -> 次数)
```

```bash
curl http://127.0.0.1:8000/metrics/commands
# { "submitted": 12, "accepted": 7, "duplicates": 1,
#   "rejected": {"COMMAND_QUEUE_FULL": 2, "COMMAND_UNAUTHORIZED": 1,
#                "DEVICE_UNKNOWN": 1},
#   "terminal": {"succeeded": 5, "failed": 1, "cancelled": 1},
#   "active": 0, "history": 7, "retired": 3, "accounts_balanced": true }
```

- `accepted`：真正入队（进入状态机）的提交；`duplicates`：同设备同 id 的
  并发/重复提交（只复用结论，不重复执行）；`rejected` 按原因码分别计数。
- `terminal` 是各终态累计数（仅在终态达成时递增，取消重复幂等不重复计）。
- `active`/`history`/`retired` 为当前占用，`accounts_balanced` 直接给出对账结论。

### 2.10 拒绝原因码（全工程统一、可区分）

| reason | 触发场景 |
| --- | --- |
| `MALFORMED_JSON` | 非法 JSON / 非法 UTF-8 |
| `NOT_AN_OBJECT` | 顶层 JSON 不是对象，或重组后负载不是对象 |
| `UNKNOWN_FRAME_TYPE` | 未知 `type` |
| `MISSING_FIELD` | 必填字段缺失（或 v2 中 `humidity` 为 null） |
| `BAD_FIELD_TYPE` | 字段类型不符（`true` 不会被当作 `1`） |
| `UNSUPPORTED_VERSION` | 版本不是 `1` / `2` |
| `BAD_FRAGMENT_INDEX` | `index/total` 越界 |
| `FRAGMENT_HEADER_CONFLICT` | 同 seq 的 total/version 在分片间不一致 |
| `DUPLICATE_FRAGMENT_CONFLICT` | 同分片序号重传内容不一致 |
| `FRAGMENT_GAP` | 残缺报文被中止（如断线） |
| `DUPLICATE_SEQ_CONFLICT` | 同 seq 被用于不同负载（一致性防线） |
| `UNKNOWN_REQUIRED_FIELD` | 设备自报必填了本版本未知字段 |
| `DEVICE_UNKNOWN` | 未注册设备上报/下发 |
| `DEVICE_ID_MISMATCH` | 连接路径设备身份与帧内 device_id 不一致（防伪造） |
| `COMMAND_UNAUTHORIZED` | 设备未被授权该指令（越权） |
| `COMMAND_UNSUPPORTED` | 指令无处理器 |
| `COMMAND_HANDLER_ERROR` | 处理器抛异常（不影响串行循环） |
| `COMMAND_QUEUE_FULL` | 设备积压超过有界上限 |
| `COMMAND_DUPLICATE` | command_id 已被其它设备使用 |
| `COMMAND_NOT_FOUND` | 指令号从未提交（或已超出墓碑保留窗被彻底遗忘） |
| `COMMAND_NOT_CANCELLABLE` | 指令已开始执行或已到终态，不可取消 |
| `COMMAND_RETIRED` | command_id 已完成并按容量规则淘汰，窗内重用拒绝（防二次执行） |

---

## 3. 版本兼容规则

版本由分片帧的 `version` 携带（当前 `"1"` / `"2"`），不同版本设备可同时接入。

| 字段 | v1 | v2 |
| --- | --- | --- |
| `temperature` | 必填，数值 | 必填，数值 |
| `location` | 必填，字符串 | 必填，字符串 |
| `humidity` | **可选**，缺省/显式 null → `null` | **必填**，不允许缺失/null |
| `firmware` | 未知（作为可选未知字段被忽略） | 可选，缺省 → `"unknown"` |

规则：

1. **可选字段可忽略**：负载中出现本版本未知的可选字段，直接忽略，不入库。
2. **必填字段不可缺失**：缺失或类型不符 → 明确拒绝（`MISSING_FIELD` /
   `BAD_FIELD_TYPE`）。
3. **未知必填字段必须明确拒绝**：设备可在负载中用 `"requires": ["字段名"]`
   声明其必填字段；若其中出现本版本未知字段，返回 `UNKNOWN_REQUIRED_FIELD`，
   **绝不按旧版本猜测解析**。
4. **缺省语义随版本稳定、可复现**：缺省值由版本表唯一决定
   （见 `gateway/telemetry.py` 的 `DEFAULTS`），与字段出现顺序、到达顺序无关，
   同一输入多次归一化结果完全一致。

---

## 4. 鉴权与并发语义

### 4.1 鉴权

- 授权表：`device_id -> 被授权指令名集合`（构造 `CommandExecutor` 时注入）。
- 下发前校验：设备不存在 → `DEVICE_UNKNOWN`；指令未授权 →
  `COMMAND_UNAUTHORIZED`（HTTP 403）。越权指令 **不会入队、不会执行**。

### 4.2 同一设备串行、保序

- 每个设备一条 **有界 FIFO 队列** + 一个工作线程。
- 同一设备的指令严格按“提交入队顺序”串行生效，前一条完成后才处理下一条，
  因此不会出现并发重复执行或状态回退/串扰。
- 不同设备使用各自独立的队列与线程，**互不阻塞**（可并行）。

### 4.3 幂等去重

- `command_id` 全局幂等：并发提交相同 `command_id`（同设备）时只有一个真正
  执行，其余提交等待并复用同一结果，响应中 `duplicate=true`。
- `command_id` 跨设备复用 → `COMMAND_DUPLICATE` 拒绝。

### 4.4 有界积压与状态容量

- 每设备队列容量 `max_queue` 可配（默认 16，环境变量 `GATEWAY_MAX_QUEUE`）。
  在途 + 排队达到上限时，新提交立即得到 `COMMAND_QUEUE_FULL`，
  **以可区分的拒绝替代无界堆积**，调用方可据此重试或告警。
- 指令状态注册表同样有界：终态结果 `max_history` 条 FIFO 保留、墓碑
  `max_retired` 条环形保留（见 2.8）。非终态记录绝不被清理。
- 每条指令提交都可在 `GET /metrics/commands` 对账，
  `submitted = accepted + duplicates + Σrejected` 恒成立（见 2.9）。

### 4.5 查询与取消（本轮新增）

- **查询**：`GET /commands/{id}` 对任意指令号给出明确、可区分、可重复的
  结论：执行中状态 / 成功 / 失败 / 已取消 / 已淘汰（`retired`）/ 不存在
  （`COMMAND_NOT_FOUND`）。
- **取消**：仅 `queued` 可取消；重复取消幂等；`running` 与终态明确拒绝；
  取消不摘队列位置，保证同设备其它指令顺序不变，且不会“既取消又执行”。
- 两个新能力都建立在上一轮的每设备串行队列与全局 `command_id` 幂等之上，
  不改变上一轮的鉴权、保序、去重、续传行为（见第 8 节兼容关系）。

---

## 5. 一致性要点小结

| 风险 | 保障 |
| --- | --- |
| 分片乱序/重复 | 按 index 落位；相同分片幂等，冲突分片拒绝 |
| 同 seq 重复上报 | `confirmed_seqs` 幂等，业务记录每 seq 至多一条 |
| 断线重连 | resume 游标驱动重传；已确认不丢不重，未确认重传补齐 |
| 版本差异 | 可选忽略、必填拒绝、未知必填拒绝、缺省随版本可复现 |
| 越权下发 | 入队前按授权表拒绝，原因独立 |
| 并发下发 | 每设备串行保序 + command_id 幂等 + 有界积压拒绝 |
| 事后查不到结论 | 状态机 + 不可变快照：终态/在途/已淘汰/不存在各自可区分、重复查询一致 |
| 误取消在执行指令 | 仅 queued 可取消；running/终态 COMMAND_NOT_CANCELLABLE |
| 取消打乱顺序 | 取消只标记不摘队列，轮到时占位跳过，其它指令相对顺序不变 |
| 既取消又执行/状态回退 | 状态只进不退；worker 跳过已取消指令；终态不可退回 |
| 状态表无界增长 | 终态 FIFO(max_history) + 墓碑环(max_retired)；非终态绝不清理 |
| 清理误伤未结束指令 | 非终态不进终态序列，取消的指令离队后才允许淘汰 |
| 老指令号被重复执行 | 保留窗/墓碑窗内重用 command_id -> COMMAND_RETIRED 拒绝 |
| 容量拒绝对不上账 | 每单恰好归类一次，submitted=accepted+duplicates+Σrejected |

---

## 6. 本地验证

```bash
# 全部单测（31 个：核心 13 + 指令生命周期 15 + 传输层 3）
uv run python -m unittest discover -s tests -v

# 仅核心网关（分片/去重/续传/版本/鉴权/并发）
uv run python -m unittest tests.test_gateway -v
# 仅本轮新增：指令查询 / 取消 / 容量 / 对账
uv run python -m unittest tests.test_commands_lifecycle -v
# 仅传输层（WebSocket + HTTP，含查询/取消/对账端点）
uv run python -m unittest tests.test_transport -v

# 编译检查
uv run python -m compileall main.py gateway tests

# 用小容量 + DEBUG 日志观察淘汰/取消/对账
GATEWAY_MAX_QUEUE=4 GATEWAY_MAX_HISTORY=8 GATEWAY_MAX_RETIRED=16 \
GATEWAY_LOG_LEVEL=DEBUG uv run uvicorn main:app --port 8000
```

单测在每个用例中打印三项内容，便于审计：

- **输入报文**：实际送入的分片/指令；
- **判定依据**：命中了哪条规则；
- **最终状态**：落库记录数、重复标记、拒绝原因码、终态状态与对账快照。

并发与取消相关用例**不靠 sleep 凑并发窗口**，而是用确定性构造：票据 0 的
处理器进入执行后置 `ready` 事件并阻塞在 `gate` 闸门，测试等到 `ready` 后才
放行排队/取消/竞争动作，因此“谁在途、谁排队、谁被取消”每次运行完全确定；
`time` 仅作为失败兜底超时，不参与结果判定。连跑多次结果一致、无忙等死等。

手工快速验证查询/取消/对账：

```bash
# 1) 查一个不存在的指令号 -> 404 COMMAND_NOT_FOUND
curl -i http://127.0.0.1:8000/commands/nope
# 2) 取消一个不存在的指令号 -> 404 COMMAND_NOT_FOUND
curl -i -X POST http://127.0.0.1:8000/commands/nope/cancel
# 3) 提交后查询状态、查看对账
curl -X POST http://127.0.0.1:8000/commands \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"device-001","command_id":"c-1","name":"reboot"}'
curl http://127.0.0.1:8000/commands/c-1
curl http://127.0.0.1:8000/metrics/commands
```

遥测分片的手工联调示例（需 `websockets` 客户端）：

```python
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://127.0.0.1:8000/ws/device-001") as ws:
        print(await ws.recv())  # resume 游标
        payload = json.dumps({"temperature": 21.5, "humidity": 40, "location": "BJ"})
        mid = len(payload) // 2
        await ws.send(json.dumps({"type": "telemetry_fragment",
            "device_id": "device-001", "seq": 1, "index": 1, "total": 2,
            "version": "2", "payload_fragment": payload[mid:]}))
        print(await ws.recv())  # complete=false
        await ws.send(json.dumps({"type": "telemetry_fragment",
            "device_id": "device-001", "seq": 1, "index": 0, "total": 2,
            "version": "2", "payload_fragment": payload[:mid]}))
        print(await ws.recv())  # complete=true

asyncio.run(main())
```

---

## 7. 代码结构

```
gateway/
  protocol.py      # 帧解析、分片头校验、统一拒绝原因码（含查询/取消/容量新码）
  reassembly.py    # 分片乱序重组/去重/残缺中止（线程安全）
  telemetry.py     # 版本兼容、必填/未知必填校验、缺省归一化
  session.py       # 会话与已确认序列号（续传游标）
  commandstore.py  # 指令状态机：查询快照、取消判定、FIFO 保留 + 墓碑环、对账计数
  commands.py      # 鉴权、每设备串行队列、幂等去重、有界积压、查询/取消入口
  gateway.py       # 门面：去重落库 + 续传 + 指令下发/查询/取消/对账
  app.py           # FastAPI：WebSocket / HTTP（含查询、取消、对账端点）
main.py            # 默认授权表与处理器装配；容量参数走环境变量
tests/             # 31 个单测（核心 13 + 指令生命周期 15 + 传输层 3）
```

---

## 8. 与上一轮能力的兼容关系

本轮新增能力**只增量、不改既有契约**：

1. **提交接口不变**：`POST /commands` 的请求体与响应字段（`command_id`、
   `status`、`duplicate`、`response`、`error`）保持原样；`CommandResult`
   新增的 `device_id` 字段有默认值，旧调用方无需改动。成功仍为 `status="ok"`，
   鉴权/积压/跨设备重复等拒绝原因码与 HTTP 403 语义不变。
2. **状态机是叠加视图**：指令五态（`queued/running/succeeded/failed/cancelled`）
   是对既有“提交并阻塞至执行完毕”结果的细化——`succeeded` 对应旧的 `ok`，
   `failed` 对应处理器层面的 `rejected`（`COMMAND_UNSUPPORTED` /
   `COMMAND_HANDLER_ERROR`），`cancelled` 为新增。
3. **串行保序与幂等不放松**：取消只标记、不摘队列，因此同设备 FIFO 顺序、
   全局 `command_id` 幂等、跨设备复用拒绝等保证与上一轮完全一致；新增的
   “墓碑窗内重用拒绝（`COMMAND_RETIRED`）”只在指令已离开活跃/保留窗后才
   生效，不影响窗内的原有去重路径。
4. **遥测链路零改动**：分片重组、去重落库、断线续传、版本兼容、WebSocket
   鉴权与身份校验均未修改，原有用例全部保留并持续通过。
5. **容量默认值向后兼容**：`max_queue` 默认仍为 16；`max_history`/
   `max_retired` 为新增可选参数（默认 256/4096），旧的 `CommandExecutor`
   构造方式（不传这两个参数）照常工作。
6. **新增端点**：`GET /commands/{id}`、`POST /commands/{id}/cancel`、
   `GET /metrics/commands` 均为新路由，不与既有路由冲突。
