"""设备会话：重连续传游标与已确认序列号。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DeviceSession:
    device_id: str
    # 已确认（业务已落库）的最大序列号
    confirmed_seq: int = 0
    # 已确认序列号集合，支持乱序确认与幂等查询
    confirmed_seqs: set[int] = field(default_factory=set)

    def is_confirmed(self, seq: int) -> bool:
        """该序列号是否已被确认（已确认数据重传时不得重复落库）。"""
        return seq in self.confirmed_seqs

    def mark_confirmed(self, seq: int) -> bool:
        """标记为已确认；返回是否为新确认（False 表示重复确认，幂等）。"""
        if seq in self.confirmed_seqs:
            return False
        self.confirmed_seqs.add(seq)
        if seq > self.confirmed_seq:
            self.confirmed_seq = seq
        return True
