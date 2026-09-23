# 报文协议约定

## 1. 连接与分帧

- 设备通过 WebSocket 长连接接入：`ws://<host>/ws/{device_id}`（也可以不接
  传输层，直接把帧对象喂给 `gateway.core.Gateway.handle_frame`，测试即如此）。
- 每个连接帧是**一个 JSON 对象**（文本帧）。网关对每个输入帧回送一个 JSON
  结果对象。
- 一条遥测业务报文可能被拆成多个连接帧（分片），以 `(session, seq)` 聚合重组。

## 2. 帧结构

```json
{
  "v": 1,
  "type": "telemetry",
  "device": "device-001",
  "session": "conn-uuid-...",
  "seq": 12,
  "frag": { "i": 0, "n": 3 },
  "payload_b64": "base64(分片0字节)",
  "resume_from": 0
}
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `v` | 是 | 协议版本，当前支持 `1`、`2`；其它值一律拒绝 `unsupported_version`。 |
| `type` | 是 | `telemetry`（遥测）或 `ack`（确认）。未知值拒绝 `unknown_message_type`。 |
| `device` | 是 | 设备 ID，非空字符串；未注册拒绝 `device_unknown`。 |
| `session` | 是 | 本次连接的会话 ID。断线重连会产生新的 session ID。 |
| `seq` | 是 | 会话内单调递增的报文序号，从 1 开始，整数 ≥ 0。 |
| `frag` | telemetry 必填 | 分片描述 `{"i": 当前索引, "n": 总分片数}`，`0 ≤ i < n`。单分片报文用 `n=1`。 |
| `payload_b64` | telemetry 必填 | **本分片字节**的标准 base64（严格模式）。 |
| `resume_from` | 重连必填 | 重连时设备本地记录的"已确认位点"，见 [续传](#5-断线重连续传)。 |

ACK 帧：`{"v":1,"type":"ack","device":"...","session":"...","seq":12}`，
不得携带 `frag/payload_b64`。

## 3. 分片重组规则

- 按 `(session_id, seq)` 聚合，收齐 `0..n-1` 后**把每个分片解码出的字节
  按索引顺序拼接**，再把完整字节做 UTF-8/JSON 解析。
- 注意：不能把多个 base64 字符串直接拼接，因为 base64 的 padding 与 3 字节
  分组边界会导致结果非法；因此每个分片独立 base64 解码。
- 分片允许**乱序**到达。
- 同一索引分片**重复到达且内容相同**：视为网络重传，幂等忽略，不报错。
- 同一索引分片重复但**内容不同**：拒绝 `fragment_conflict`。
- 同一 `(session, seq)` 的 `n` 前后不一致：拒绝 `fragment_total_mismatch`。
- `i >= n`：拒绝 `fragment_index_invalid`。
- 任一分片不是合法 base64：拒绝 `payload_not_base64`。
- 重组后字节不是合法 JSON / 顶层不是对象：分别拒绝
  `payload_not_json` / `payload_not_object`。

## 4. 遥测 payload 与版本兼容

重组后的 payload 是一个 JSON 对象。

| 字段 | v1 | v2 |
| --- | --- | --- |
| `ts`（数值时间戳） | 必填 | 必填 |
| `metrics`（非空对象） | 必填 | 必填 |
| `quality` | 可选，缺省 `0` | 可选，缺省 `1` |
| `tags` | 可选，缺省 `[]` | 可选，缺省 `[]` |
| `location` | 未知→作为未知**可选**字段忽略 | 可选 |

兼容规则（关键）：

1. **不同版本可同时在线**，版本号取自帧的 `v`，校验规则按 `v` 选择。
2. 必填字段缺失 / 为 null：拒绝 `missing_required_field` / `bad_field_type`。
3. **未知版本号整体拒绝**（`unsupported_version`），绝不按旧版本猜测解析。
4. **未知的必填扩展必须明确拒绝**：高版本设备可在 payload 内用
   `"__required_ext__": ["字段名"]` 声明"网关必须认识的新增必填字段"；
   若该字段不在当前版本的已知字段集合中，拒绝 `unknown_required_field`，
   不会按旧协议静默解析。
5. 未在 `__required_ext__` 中声明的未知字段一律视为**未知可选字段**，直接
   忽略，不影响受理。
6. **缺省语义由版本号固定**：同一份输入在同一版本下，归一化结果永远一致
   （可复现）。

## 5. 断线重连续传

- 网关为每个设备维护：`confirmed_seq`（已通过 ACK 确认的最高序号）与
  `accepted_seqs`（已接收并写入的序号集合），记录在断线期间保留。
- 设备以**新的 session ID** 重连时，必须携带 `resume_from`（设备本地记录的
  已确认位点）。网关校验：
  - `resume_from > 网关已接收最高序号`：设备声称的某些数据网关从未见过，
    拒绝 `resume_from_mismatch`（防止静默丢数据）；
  - `resume_from < confirmed_seq`：设备状态回退，拒绝
    `resume_from_mismatch`（防止重复/状态回退）；
  - 一致才绑定新会话。不带 `resume_from` 的新会话同样拒绝。
- 重连前未集齐的分片缓存会作废，设备需在新会话中重发该报文（幂等：已写入
  的 seq 重放只会得到 `duplicate=true`，不会产生第二条业务记录）。
- 已确认的数据不丢失：业务记录按设备持久保留在会话状态中（本实现为内存态，
  接口层可替换为持久存储）。

## 6. 拒绝原因一览（可区分）

| code | 含义 |
| --- | --- |
| `frame_not_json` / `frame_not_object` | 帧不是合法 JSON / 顶层不是对象 |
| `missing_required_field` / `bad_field_type` | 缺必填字段 / 字段类型错误 |
| `unknown_message_type` | 未知 type |
| `unsupported_version` | 版本号不支持 |
| `unknown_required_field` | 存在网关不认识的必填扩展 |
| `fragment_index_invalid` | `i>=n` 等索引非法 |
| `fragment_total_mismatch` | 同序号分片总数前后不一致 |
| `fragment_conflict` | 同索引分片重复但内容不同 |
| `payload_not_base64` / `payload_not_json` / `payload_not_object` | payload 解码各阶段失败 |
| `resume_from_mismatch` | 续传位点不匹配（可能丢数或状态回退） |
| `device_unknown` | 设备未注册 |
| `unauthorized_command` | 设备无权执行该下行指令 |
| `command_queue_full` | 该设备指令积压超过上限 |
| `command_duplicate_request` | （内部码）请求去重由结果中的 `duplicate=true` 表达 |
