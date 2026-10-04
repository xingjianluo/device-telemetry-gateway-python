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

#### 2.4.1 事后查询指令状态 `GET /commands/{command_id}`

指令提交后，可随时凭 `command_id` 查询其**稳定终态**，反复查询结论一致：

```bash
curl http://127.0.0.1:8000/commands/c-1
# 成功
# { "command_id":"c-1", "found":true, "device_id":"device-001",
#   "name":"reboot", "status":"succeeded", "response": {...} }
# 被拒绝（含原因）
# { ..., "status":"rejected", "error":{"reason":"COMMAND_UNSUPPORTED", ...} }
# 从未提交过（或已超保留窗口被彻底清理）
# HTTP 404 { "found":false,
#   "error":{"reason":"COMMAND_NOT_FOUND","detail":"..."} }
```

- 成功 / 被拒绝（各原因码） / 被取消 / 无此号，四类答复**相互可区分**。
- 被拒绝的提交（鉴权失败、队列满、容量到顶等）也会在保留窗口内留痕可查；
  重复提交同号查到的是首条结论，查询本身不会被标成 `duplicate`。

#### 2.4.2 撤销未开始执行的指令 `POST /commands/{command_id}/cancel`

```bash
curl -X POST http://127.0.0.1:8000/commands/c-1/cancel
# 排队中，撤销成功：      200 {"status":"canceled","canceled":true,"idempotent":false}
# 重复撤销（幂等）：       200 {"status":"canceled","canceled":true,"idempotent":true}
# 已开始执行/已完成：      409 {"canceled":false,
#                              "error":{"reason":"COMMAND_CANCEL_TOO_LATE", ...}}
# 未知指令号：            404 {"canceled":false,
#                              "error":{"reason":"COMMAND_NOT_FOUND", ...}}
```

- 只有**尚未开始执行**（排队中 `pending`）的指令能撤销；
- 撤销在工作线程"取到指令、准备执行"的同一状态机判定上原子完成，
  因此不可能出现一条指令**既被取消又被执行**；被取消的指令被直接跳过，
  **同一台设备其它指令的先后顺序不受影响**；
- 状态只有前向流转（见 §4.5），不会从已完成退回未完成。

#### 2.4.3 容量与对账 `GET /commands-metrics/details`

返回提交/受理/复用/各类拒绝/撤销/清理的累计计数，用于对账，恒等式见 §4.6。

### 2.5 拒绝原因码（全工程统一、可区分）

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
| `COMMAND_NOT_FOUND` | 查询/撤销了一个从未提交过（或已超窗遗忘）的指令号 |
| `COMMAND_CANCELED` | 指令在开始执行前被撤销（终态） |
| `COMMAND_CANCEL_TOO_LATE` | 指令已开始执行或已到终态，撤销被拒（HTTP 409） |
| `COMMAND_STATUS_LIMIT` | 指令状态登记册到顶且没有可清理的终态 |

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

### 4.4 有界积压

- 队列容量 `max_queue` 可配（默认 16，环境变量 `GATEWAY_MAX_QUEUE`）。
- 在途 + 排队达到上限时，新的并发提交立即得到 `COMMAND_QUEUE_FULL`，
  **以可区分的拒绝替代无界堆积**，调用方可据此重试或告警。

### 4.5 指令状态机与撤销

每条指令在 `CommandRegistry` 中有单向状态，**不存在回退边**：

```
                 鉴权即拒 / 容量到顶
   (提交) ───────────────────────────────▶ rejected(终态)
     │
     ▼
  pending ──撤销成功──▶ canceled(终态)        # 唯一可撤销窗口：排队中
     │                                       # 重复撤销 -> 幂等
     │ 工作线程取到、准备执行（原子 CAS）
     ▼
 executing ─撤销──▶ COMMAND_CANCEL_TOO_LATE  # 窗口已关闭，拒绝撤销
     │
     ├──处理器成功──▶ succeeded(终态)
     └──无处理器/抛异常─▶ rejected(终态, COMMAND_UNSUPPORTED /
                                   COMMAND_HANDLER_ERROR)
```

要点：

- **原子判定**：撤销与"开始执行"在同一把锁内竞争，结局互斥。被撤销的排队
  指令由工作线程在 CAS 处发现并直接跳过——既不调用处理器，也不改变同设备
  其它指令的 FIFO 顺序。
- **终态稳定可查**：终态不可变，同一 `command_id` 反复查询结论一致；
  提交者、并发重复提交者、事后查询者看到的是同一结论。
- 已开始执行 / 已完成（`executing`/`succeeded`/`rejected`）的撤销请求一律
  返回 `COMMAND_CANCEL_TOO_LATE`，状态绝不从已完成退回。

### 4.6 有界容量、清理规则与对账

网关长时间不重启，指令状态不能只进不出。`CommandRegistry(max_records=...)`
（默认 4096，环境变量 `GATEWAY_MAX_STATUS_RECORDS`）给出**明确上限**：

1. **只清理终态**：`pending`/`executing` 等未结束指令永远不会被当成不存在，
   因此清理不会导致同一条指令被重复执行，也不会让等待者丢结论。
2. **两级回收（按首次提交先后，最旧优先，规则确定可解释）**：
   - 完整记录数到顶时，把**最旧的终态完整记录压缩成墓碑（tombstone）**：
     只保留"指令号 → 可区分结论（含成功/拒绝/取消原因）"，丢弃响应体；
     墓碑仍占去重窗口，窗口内同号重复提交照样幂等、不二次执行。
   - 墓碑也到上限时，**最旧墓碑被彻底遗忘**；此后再查该号得到明确的
     `COMMAND_NOT_FOUND`（与"从未提交"同一可区分答复，不含糊）。
3. **到顶该拒就拒**：若占满容量的全是未结束指令（没有任何终态可清理），
   新提交立即得到 `COMMAND_STATUS_LIMIT`，绝不无界增长、绝不误删在途指令。
   （容量上限按在途指令规模配置；正常业务下终态随时可回收。）

对账（`GET /commands-metrics/details`，代码中 `check_command_invariants()` 断言）：

- **提交账**：`submitted == accepted + reused_duplicate
  + rejected_device_unknown + rejected_unauthorized
  + rejected_duplicate + rejected_status_limit`
- **受理账**：`accepted == 当前未结束数 + succeeded + canceled
  + queue_full + unsupported + handler_error`
  （`COMMAND_QUEUE_FULL` 是"已受理但设备 FIFO 放不下"，故计入 accepted 后落终态）
- **容量账**：完整记录数、墓碑数恒不超过上限；墓碑里只有终态。

### 4.7 与上一轮行为的兼容关系

- 分片重组、乱序/重复去重、残缺 `FRAGMENT_GAP`、断线重连续传、版本兼容、
  连接身份校验等**上报链路完全未改动**（既有测试全部保留）。
- 指令仍为"每设备 FIFO 串行 + 全局 `command_id` 幂等 + 有界队列"；
  变化只是把原先"提交完即丢"的指令结果改为**在有界登记册中保留终态**，
  并新增查询 / 撤销 / 容量对账。`POST /commands` 的请求体、成功与
  `COMMAND_*` 拒绝响应结构保持不变；新增的 `canceled` 状态同样以 HTTP 200
  返回（它是一个明确终态而非错误），真正的拒绝仍为 403、撤销冲突为 409、
  无此号为 404。

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
| 事后追溯 | 终态在有界登记册保留，凭 command_id 稳定查询，无此号明确 NOT_FOUND |
| 撤销指令 | 仅 pending 可撤销且幂等；与开始执行原子竞争，不重复执行、不乱序、不回退 |
| 长期容量 | 终态"压缩墓碑→超窗遗忘"有界回收；只清终态；全在途时 STATUS_LIMIT；账可对平 |

---

## 6. 本地验证

```bash
# 全部单测（分片/去重/续传/版本/鉴权/并发 + 查询/撤销/容量 + WS/HTTP 集成）
uv run python -m unittest discover -s tests -v

# 仅新增的指令生命周期（查询/撤销/容量对账）——确定性并发，约 0.01s
uv run python -m unittest tests.test_command_lifecycle -v
# 仅核心网关（含上一轮并发/串行/跨设备）
uv run python -m unittest tests.test_gateway -v
# 仅传输层（含新增 GET 查询 / cancel / metrics 端点）
uv run python -m unittest tests.test_transport -v

# 编译检查
uv run python -m compileall main.py gateway tests

# 容量上限的环境变量验证
GATEWAY_MAX_QUEUE=2 GATEWAY_MAX_STATUS_RECORDS=4 \
  uv run uvicorn main:app --port 8000
```

单测在每个用例中打印三项内容，便于审计：

- **输入报文**：实际送入的分片/指令；
- **判定依据**：命中了哪条规则；
- **最终状态**：落库记录数、重复标记、拒绝原因码等。

启动服务后的手工联调示例（需 `websockets` 客户端）：

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
  protocol.py    # 帧解析、分片头校验、统一拒绝原因码
  reassembly.py  # 分片乱序重组/去重/残缺中止（线程安全）
  telemetry.py   # 版本兼容、必填/未知必填校验、缺省归一化
  session.py     # 会话与已确认序列号（续传游标）
  registry.py    # 指令状态机 + 有界登记册：查询/撤销/墓碑回收/容量对账
  commands.py    # 鉴权、每设备串行队列、幂等去重、有界积压、查询与撤销
  gateway.py     # 门面：去重落库 + 续传 + 指令下发/查询/撤销/对账
  app.py         # FastAPI：WebSocket / HTTP 传输层（含 query/cancel/metrics）
main.py          # 默认授权表、处理器与容量上限的装配（支持环境变量）
tests/           # 28 个单测（核心 23 + 传输层 5）
```
