"""压缩样例的 CPU 计量：/proc 实时采样 + wait4 终值落账。

设计要点（对应“刷新间隙退出也要计入 / 交接只增不减 / 每份只计一次”）：

* 每个压缩子进程在 *运行期间* 由 worker 读 ``/proc/<pid>/stat`` 的
  utime+stime（clock ticks）得到实时值，只用于页面展示；
* 子进程退出的瞬间，worker 用 ``os.wait4(pid, WNOHANG)`` 回收，
  同时拿到内核给出的 ``rusage.ru_utime + ru_stime`` 终值。
  无论子进程是否落在网页两次刷新之间，这笔终值都会被提交；
* 已回收子进程的用量进入 committed（只加一次，由 ``accounted`` 守门），
  当前子进程的 /proc 采样只是 *替换* 活动项而不是累加，因此不会重复计数；
* wait4 的微秒级终值不小于最后一次 /proc tick 采样（tick 向下取整），
  交接时用终值替换活动采样，总数天然不回落；对外输出再做一次
  “不小于上一次发布值”的钳制，保证任何平台抖动下都只增不减。
"""
from __future__ import annotations

import os
from typing import Any

# 各负载档对应的“每个子进程目标 CPU 秒数”。
# quick 只压一个小块，生命周期主要是解释器启动，通常远小于网页 2s 刷新；
# standard 留给页面观察实时增长；long 供手动中断 / 重启演练。
PROFILES: dict[str, dict[str, float | int]] = {
    "quick": {"target_cpu": 0.0, "chunk_mb": 1, "max_rounds": 1,
              "max_wall": 30.0},
    "standard": {"target_cpu": 2.0, "chunk_mb": 8, "max_rounds": 10000,
                 "max_wall": 120.0},
    "long": {"target_cpu": 15.0, "chunk_mb": 8, "max_rounds": 1_000_000,
             "max_wall": 600.0},
}
DEFAULT_PROFILE = "standard"


def normalize_profile(value: Any) -> str:
    name = str(value or "").strip().lower()
    aliases = {
        "fast": "quick",
        "quick_exit": "quick",
        "quick-exit": "quick",
        "smoke": "quick",
        "small": "quick",
        "normal": "standard",
        "std": "standard",
        "slow": "long",
        "heavy": "long",
    }
    name = aliases.get(name, name)
    return name if name in PROFILES else DEFAULT_PROFILE


def clock_ticks() -> int:
    try:
        return int(os.sysconf("SC_CLK_TCK"))
    except (ValueError, OSError, AttributeError):
        return 100


def read_proc_cpu(pid: int) -> tuple[float, float, float] | None:
    """返回 (user+system, user, system) 秒；进程已不在 /proc 时返回 None。

    /proc/<pid>/stat 中 comm 字段可能含空格/括号，必须从最后一个 ')' 后切。
    ')' 之后第 0 列是 state（总第 3 字段），故总第 14/15 字段 utime/stime
    对应切分后的下标 11/12。
    """
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as fh:
            data = fh.read()
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None
    rparen = data.rfind(")")
    if rparen < 0:
        return None
    parts = data[rparen + 2:].split()
    try:
        utime_ticks = int(parts[11])
        stime_ticks = int(parts[12])
    except (IndexError, ValueError):
        return None
    tick = clock_ticks()
    user = utime_ticks / tick
    system = stime_ticks / tick
    return user + system, user, system


class ChildMeter:
    """单个压缩子进程的计量账。"""

    __slots__ = ("index", "pid", "accounted", "live_cpu", "live_user",
                 "live_system", "cpu_seconds", "user_seconds",
                 "system_seconds", "source")

    def __init__(self, index: int) -> None:
        self.index = index
        self.pid: int | None = None
        self.accounted = False
        self.live_cpu = 0.0
        self.live_user = 0.0
        self.live_system = 0.0
        # wait4 终值（提交后非 None）
        self.cpu_seconds: float | None = None
        self.user_seconds: float | None = None
        self.system_seconds: float | None = None
        self.source: str | None = None

    def to_dict(self) -> dict:
        if self.accounted:
            return {
                "index": self.index,
                "pid": self.pid,
                "state": "accounted",
                "accounted": True,
                "counted_once": True,
                "source": self.source or "wait4",
                "cpu_seconds": round(self.cpu_seconds or 0.0, 6),
                "user_seconds": round(self.user_seconds or 0.0, 6),
                "system_seconds": round(self.system_seconds or 0.0, 6),
            }
        return {
            "index": self.index,
            "pid": self.pid,
            "state": "running" if self.pid else "pending",
            "accounted": False,
            "counted_once": False,
            "source": "/proc/stat",
            "cpu_seconds": round(self.live_cpu, 6),
            "user_seconds": round(self.live_user, 6),
            "system_seconds": round(self.live_system, 6),
        }


class CompressionMeter:
    """一个压缩样例任务（两个顺序子进程）的累计账。"""

    def __init__(self, profile: str = DEFAULT_PROFILE) -> None:
        self.sample = True
        self.profile = normalize_profile(profile)
        self.children = [ChildMeter(1), ChildMeter(2)]
        self.active_index: int | None = None
        self.status = "pending"      # pending | running | complete | incomplete
        self.complete = False
        self.interrupted = False
        self.note: str | None = None
        # 已 wait4 提交部分（终值之和）
        self.committed_cpu = 0.0
        self.committed_user = 0.0
        self.committed_system = 0.0
        # 上一次对外发布值，用于单调性钳制
        self._last_total = 0.0
        self._last_user = 0.0
        self._last_system = 0.0

    # ------------------------------------------------------------------ #
    def start_child(self, index: int, pid: int) -> None:
        child = self.children[index - 1]
        child.pid = pid
        self.active_index = index
        self.status = "running"

    def sample_active(self) -> None:
        child = self._active_child()
        if child is None or child.pid is None or child.accounted:
            return
        stats = read_proc_cpu(child.pid)
        if stats is None:
            # /proc 暂时消失：可能已退出但尚未 wait4，保留上一次采样即可
            return
        total, user, system = stats
        # /proc 计数本身单调，这里也只增不减
        child.live_cpu = max(child.live_cpu, total)
        child.live_user = max(child.live_user, user)
        child.live_system = max(child.live_system, system)

    def account_reaped(self, pid: int, rusage: Any) -> ChildMeter | None:
        """用 wait4 得到的 rusage 提交一个已回收子进程。

        同一笔账只提交一次：重复调用（理论上不会，wait4 只返回一次）
        或 pid 不匹配都不会重复累加。
        """
        child = next((c for c in self.children
                      if c.pid == pid and not c.accounted), None)
        if child is None:
            return None
        user = float(getattr(rusage, "ru_utime", 0.0) or 0.0)
        system = float(getattr(rusage, "ru_stime", 0.0) or 0.0)
        total = user + system
        # 单调交接保险：终值理论上 >= 最后 tick 采样；取 max 防止任何
        # 平台性的反向取整造成页面总数回落。
        total = max(total, child.live_cpu)
        user = max(user, child.live_user)
        system = max(system, child.live_system)
        child.user_seconds = user
        child.system_seconds = system
        child.cpu_seconds = total
        child.accounted = True
        child.source = "wait4+rusage"
        self.committed_user += user
        self.committed_system += system
        self.committed_cpu += total
        child.live_cpu = child.live_user = child.live_system = 0.0
        self.active_index = None
        return child

    def mark_complete(self) -> None:
        self.complete = True
        self.status = "complete"
        self.interrupted = False
        self.note = "两个压缩子进程均已 wait4 回收，用量计全"
        # 终态发布值必须等于提交账之和
        self._last_user = self.committed_user
        self._last_system = self.committed_system
        self._last_total = self.committed_cpu

    def mark_interrupted(self, note: str) -> None:
        self.complete = False
        self.status = "incomplete"
        self.interrupted = True
        self.note = note

    # ------------------------------------------------------------------ #
    def _active_child(self) -> ChildMeter | None:
        if self.active_index is None:
            return None
        return self.children[self.active_index - 1]

    def totals(self) -> tuple[float, float, float]:
        """当前应展示的 (total, user, system)，带单调钳制。"""
        active = self._active_child()
        if active is not None and not active.accounted:
            cand_user = self.committed_user + active.live_user
            cand_system = self.committed_system + active.live_system
        else:
            cand_user = self.committed_user
            cand_system = self.committed_system
        self._last_user = max(self._last_user, cand_user)
        self._last_system = max(self._last_system, cand_system)
        total = max(self._last_total, self._last_user + self._last_system,
                    self.committed_cpu)
        self._last_total = total
        return total, self._last_user, self._last_system

    def accounted_count(self) -> int:
        return sum(1 for c in self.children if c.accounted)

    def to_dict(self) -> dict[str, Any]:
        total, user, system = self.totals()
        if self.complete:
            label = "已计全"
        elif self.status == "incomplete":
            label = "未计全"
        elif self.status == "running":
            label = "统计中"
        else:
            label = "待开始"
        return {
            "sample": True,
            "enabled": True,
            "kind": "cpu-compression-sample",
            "profile": self.profile,
            "status": self.status,
            # complete / fully_counted / accounting_complete 同义，
            # 方便不同调用方按字段判定“是否计全”
            "complete": self.complete,
            "fully_counted": self.complete,
            "accounting_complete": self.complete,
            "count_complete": self.complete,
            "is_full": self.complete,
            "interrupted": self.interrupted,
            "count_label": label,
            "complete_label": ("计全" if self.complete else "未计全"),
            "children_total": 2,
            "children_accounted": self.accounted_count(),
            "active_index": self.active_index,
            "active_pid": (self._active_child().pid
                           if self._active_child() else None),
            # 主指标：用户态 + 内核态 CPU 秒数，从 0 开始
            "cpu_seconds": round(total, 6),
            "cpu_time": round(total, 6),
            "cpu_time_seconds": round(total, 6),
            "total_cpu_seconds": round(total, 6),
            "user_seconds": round(user, 6),
            "system_seconds": round(system, 6),
            "user_cpu_seconds": round(user, 6),
            "kernel_seconds": round(system, 6),
            "kernel_cpu_seconds": round(system, 6),
            "committed_cpu_seconds": round(self.committed_cpu, 6),
            "note": self.note,
            "accounting": "wait4 rusage per reaped child + "
                          "/proc/stat utime+stime for the live child",
            "children": [c.to_dict() for c in self.children],
        }

    def top_level(self) -> dict[str, Any]:
        view = self.to_dict()
        return {
            "compression_cpu_seconds": view["cpu_seconds"],
            "compression_cpu_time": view["cpu_seconds"],
            "compression_cpu_time_seconds": view["cpu_seconds"],
            "compression_complete": self.complete,
            "compression_fully_counted": self.complete,
            "compression_count_complete": self.complete,
            "compression_status": self.status,
            "compression_count_label": view["count_label"],
        }


def incomplete_view(existing: dict | None, note: str) -> dict:
    """把磁盘上已有的 compression 账标记为未计全（重启回收/异常退出用）。

    保留最后记录的 cpu_seconds 与 children，绝不清零。
    """
    comp = dict(existing) if isinstance(existing, dict) else {}
    comp.setdefault("sample", True)
    comp.setdefault("enabled", True)
    comp["complete"] = False
    comp["fully_counted"] = False
    comp["accounting_complete"] = False
    comp["status"] = "incomplete"
    comp["interrupted"] = True
    comp["count_label"] = "未计全"
    comp["complete_label"] = "未计全"
    comp["final_note"] = note
    return comp
