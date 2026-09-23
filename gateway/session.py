"""设备会话：序列号去重、确认位点与断线续传。

序列号模型：
- 每个会话内遥测报文从 seq=1 起单调递增；``confirmed_seq`` 表示网关已
  通过 ACK 确认的最高序号（0 表示尚无确认）。
- 同一 session 内同一 seq 只接受一次：重复上报不再产生业务记录
  （``accept`` 返回 False，调用方据此标记 duplicate）。
- 重连时设备携带 ``resume_from``（即它本地记录的 confirmed_seq）。
  网关必须与自身确认位点一致：
  * 设备声称的位点高于网关已接受位点 -> 设备侧有网关从未见过的数据，
    拒绝 ``resume_from_mismatch``，防止静默丢数据；
  * 设备声称的位点低于网关已确认位点 -> 设备回退到旧状态，同样拒绝，
    防止状态回退导致重复/丢失。
- 已确认记录在断线期间保留，``records`` 不随断开清理。
"""

from __future__ import annotations

import threading

from .errors import GatewayError, RejectCode


def _err(code: RejectCode, message: str) -> GatewayError:
    return GatewayError(message, code=code)


class SessionState:
    """单个设备会话的状态（线程安全）。"""

    def __init__(self, device_id: str, session_id: str, resume_from: int = 0) -> None:
        self.device_id = device_id
        self.session_id = session_id
        self.confirmed_seq = 0
        self.accepted_seq: set[int] = set()
        self.records: dict[int, dict] = {}
        self.online = True
        self._lock = threading.Lock()
        if resume_from:
            self.check_resume(resume_from)

    def check_resume(self, claimed_confirmed_seq: int) -> None:
        """重连时校验对端声称的已确认位点是否与网关一致。"""
        if claimed_confirmed_seq < 0:
            raise _err(RejectCode.RESUME_FROM_MISMATCH, "resume_from 不能为负")
        with self._lock:
            highest = max(self.accepted_seq, default=0)
            if claimed_confirmed_seq > highest:
                raise _err(
                    RejectCode.RESUME_FROM_MISMATCH,
                    f"续传位点 {claimed_confirmed_seq} 高于网关已接受最高位点 {highest}："
                    "存在网关从未收到的数据，拒绝续传以防丢失",
                )
            if claimed_confirmed_seq < self.confirmed_seq:
                raise _err(
                    RejectCode.RESUME_FROM_MISMATCH,
                    f"续传位点 {claimed_confirmed_seq} 低于网关已确认位点 {self.confirmed_seq}："
                    "设备状态回退，拒绝续传以防重复",
                )

    def rebind(self, session_id: str) -> None:
        """设备以新会话重连并通过位点校验后，绑定新的 session_id。"""
        with self._lock:
            self.session_id = session_id
            self.online = True

    def is_duplicate(self, seq: int) -> bool:
        with self._lock:
            return seq in self.accepted_seq

    def accept(self, seq: int, record: dict) -> bool:
        """原子地登记一条业务记录；重复 seq 返回 False 且不写入。"""
        with self._lock:
            if seq in self.accepted_seq:
                return False
            self.accepted_seq.add(seq)
            self.records[seq] = record
            return True

    def confirm(self, seq: int) -> bool:
        """确认位点推进（收到 ACK 后调用）。

        只能确认已接受的 seq；确认一个更高位点会把中间位点一并确认。
        返回该 ACK 是否为新的确认（重复 ACK 返回 False）。
        """
        with self._lock:
            if seq not in self.accepted_seq:
                raise _err(
                    RejectCode.MISSING_REQUIRED_FIELD,
                    f"无法确认未接受的 seq={seq}",
                )
            if seq <= self.confirmed_seq:
                return False
            self.confirmed_seq = seq
            return True

    def snapshot(self) -> dict:
        """导出可解释的当前状态，供日志与测试断言。"""
        with self._lock:
            return {
                "device_id": self.device_id,
                "session_id": self.session_id,
                "online": self.online,
                "confirmed_seq": self.confirmed_seq,
                "accepted_seqs": sorted(self.accepted_seq),
                "record_count": len(self.records),
                "record_seqs": sorted(self.records),
            }
