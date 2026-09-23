# 设备接入网关（Device Telemetry Gateway）

多台设备通过长连接上报遥测数据、接收下行指令。面向不稳定网络（分片、乱序、
重复、断线重连）与高并发下发，目标是**结果一致、失败可解释**。

## 模块结构

```
gateway/
  errors.py     # 统一错误码 RejectCode 与 GatewayError
  protocol.py   # 帧/分片/指令等数据类型
  codec.py      # 帧解析、base64/JSON 解码、版本兼容与必填校验
  assembler.py  # 分片重组（乱序、重传幂等、冲突检测）
  session.py    # 会话状态：seq 去重、ACK 位点、断线续传校验
  registry.py   # 设备注册表与指令授权
  commands.py   # 下行指令总线：鉴权、串行 worker、去重、有界队列
  core.py       # 网关核心门面（不依赖网络框架）
  transport.py  # 可选 FastAPI/WebSocket 传输层
tests/          # 单测：分片/去重/续传/版本/越权/并发/端到端/WS
docs/
  protocol.md   # 报文约定、版本兼容、续传规则、拒绝码表
  commands.md   # 鉴权与并发语义
main.py         # FastAPI 入口
```

核心逻辑全部在 `gateway/`，不绑定网络框架，可直接以普通函数调用方式验证。

## 本地验证

```bash
# 1. 安装依赖（需要 Python 3.14；使用 uv）
uv sync

# 2. 运行全部单测；-s 可查看每个用例打印的
#    “输入报文 / 判定依据 / 最终状态”
uv run pytest -q
uv run pytest -s                       # 查看完整可解释输出
uv run pytest tests/test_commands.py   # 只跑并发/鉴权

# 3. 启动 HTTP/WS 服务
uv run uvicorn main:app --host 0.0.0.0 --port 8000
#    健康检查:  curl http://127.0.0.1:8000/
#    设备长连:  ws://127.0.0.1:8000/ws/{device_id}
#    下行指令:  curl -X POST http://127.0.0.1:8000/devices/d1/commands \
#                 -H 'Content-Type: application/json' \
#                 -d '{"command":"set_level","request_id":"r1","args":{"level":3}}'
```

## 快速编程式示例

```python
from gateway.core import Gateway
from gateway.registry import DeviceRegistry

reg = DeviceRegistry()
reg.register("d1", {"set_level"})
gw = Gateway(reg, max_pending=16)

# 上报（单分片 n=1；多分片时按 i/n 发送，可乱序、可重传）
gw.handle_frame({
    "v": 1, "type": "telemetry", "device": "d1", "session": "s1", "seq": 1,
    "frag": {"i": 0, "n": 1},
    "payload_b64": "eyJ0cyI6IDEsICJtZXRyaWNzIjogeyJ0IjogMX19",
})
# -> accepted=True, completed=True
# 同一 seq 再发一次 -> accepted=True, duplicate=True（不产生新记录）

# 设备确认位点
gw.handle_frame({"v": 1, "type": "ack", "device": "d1", "session": "s1", "seq": 1})

# 断线后用新会话携带 resume_from 续传
gw.on_disconnect("d1")
gw.handle_frame({..., "session": "s2", "seq": 2, "resume_from": 1})

# 状态观测
gw.state_snapshot("d1")
# -> {confirmed_seq, accepted_seqs, record_count, record_seqs, ...}

# 下行指令（await）
await gw.send_command(CommandRequest("d1", "set_level", "req-1", {"level": 3}))
```

## 保证的语义

- **不重复**：相同 `(session, seq)` 只生成一条业务记录；相同
  `(device, request_id)` 指令只执行一次。
- **不丢失**：分片可乱序/重传并重组；ACK 位点与记录在断线后保留；续传位点
  不匹配（可能丢数或回退）时明确拒绝而不是静默接受。
- **可解释**：每类非法输入/越权/积压都有独立 `RejectCode`，回执携带
  `reason`，测试打印输入、判定依据与最终状态。
- **版本安全**：只接受已知版本；未知必填扩展拒绝；未知可选字段忽略；缺省值
  由版本固定且可复现。
- **并发可控**：同设备指令串行保序，跨设备并行，积压有界。

详见 [docs/protocol.md](docs/protocol.md) 与 [docs/commands.md](docs/commands.md)。
