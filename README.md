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

- 队列容量 `max_queue` 可配（默认 16）。
- 在途 + 排队达到上限时，新的并发提交立即得到 `COMMAND_QUEUE_FULL`，
  **以可区分的拒绝替代无界堆积**，调用方可据此重试或告警。

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

---

## 6. 本地验证

```bash
# 全部单测（含分片/去重/续传/版本/鉴权/并发 + WebSocket/HTTP 集成测试）
uv run python -m unittest discover -s tests -v

# 仅核心网关
uv run python -m unittest tests.test_gateway -v
# 仅传输层
uv run python -m unittest tests.test_transport -v

# 编译检查
uv run python -m compileall main.py gateway tests
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
  commands.py    # 鉴权、每设备串行队列、幂等去重、有界积压
  gateway.py     # 门面：去重落库 + 续传 + 指令下发
  app.py         # FastAPI：WebSocket / HTTP 传输层
main.py          # 默认授权表与处理器装配
tests/           # 15 个单测（核心 13 + 传输层 2）
```
