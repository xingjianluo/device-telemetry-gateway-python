"""测试公共辅助：构造报文与统一的"输入/判定/状态"打印。"""

from __future__ import annotations

import base64
import json
from typing import Any


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def telemetry_frame(
    seq: int,
    payload: Any | None = None,
    *,
    device: str = "d1",
    session: str = "s1",
    version: int = 1,
    i: int = 0,
    n: int = 1,
    raw_payload: bytes | None = None,
    **extra: Any,
) -> dict:
    if raw_payload is None:
        raw_payload = json.dumps(payload).encode("utf-8")
    frame: dict = {
        "v": version,
        "type": "telemetry",
        "device": device,
        "session": session,
        "seq": seq,
        "frag": {"i": i, "n": n},
        "payload_b64": b64(raw_payload),
    }
    frame.update(extra)
    return frame


def ack_frame(seq: int, *, device: str = "d1", session: str = "s1", version: int = 1) -> dict:
    return {"v": version, "type": "ack", "device": device, "session": session, "seq": seq}


def dump_case(title: str, raw_frame: Any, result: Any, snapshot: Any = None) -> None:
    """打印输入报文、判定依据与最终状态（满足可解释性要求）。"""
    print("\n" + "=" * 72)
    print(f"用例: {title}")
    print(f"输入报文: {json.dumps(raw_frame, ensure_ascii=False, sort_keys=True)}")
    if hasattr(result, "to_dict"):
        rd = result.to_dict()
    elif isinstance(result, dict):
        rd = result
    else:
        rd = {"repr": repr(result)}
    print(f"判定依据: accepted={rd.get('accepted')} duplicate={rd.get('duplicate')} "
          f"completed={rd.get('completed')} code={rd.get('code')} reason={rd.get('reason')}")
    if snapshot is not None:
        print(f"最终状态: {json.dumps(snapshot, ensure_ascii=False, sort_keys=True)}")
