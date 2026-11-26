"""样例备份任务（独立子进程）。

行为模拟一个定时备份作业：
  1. 在任务状态文件中登记 queued -> waiting；
  2. 对 *当前* 锁文件用 fcntl.flock(LOCK_EX) 做真实的阻塞式排他锁；
  3. 拿到锁后持锁运行指定时长（模拟备份过程），状态置为 running；
  4. 结束后释放锁并置为 done / error。

关键细节：每一轮取锁都重新 open() 当前锁路径，并在拿到锁之后再次核对
(a) 检修闸门 (b) 文件 inode 是否仍是打开时的那一份。这样可以：
  - 检修期间等待中的任务不会趁闸门打开而抢跑；
  - 锁文件在等待期间被替换时，任务只会锁到替换后的最新文件，
    而不是缓存旧 fd 长期占着已经被换掉的旧文件。
"""
from __future__ import annotations

import argparse
import fcntl
import os
import signal
import sys
import time
from datetime import datetime, timezone

from . import config
from .common import atomic_write_json, file_identity, read_json, read_maintenance

_shutdown = False


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    _shutdown = True


def main() -> int:
    parser = argparse.ArgumentParser(description="样例备份 worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--duration", type=float, required=True)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    state_path = os.path.join(config.STATE_DIR, f"{args.job_id}.json")

    def write(state: str, **extra) -> None:
        payload = {
            "job_id": args.job_id,
            "pid": os.getpid(),
            "state": state,
            "duration": args.duration,
            "updated_at": _now(),
            **extra,
        }
        atomic_write_json(state_path, payload)

    write("queued", wait_reason=None, locked_ino=None, locked_dev=None,
          locked_path=None, note="任务已登记，等待派发闸门")

    fd: int | None = None
    try:
        # ---- 取锁阶段：非阻塞轮询，便于在等待中持续检查检修闸门 --------
        write("waiting", wait_reason="gate", note="等待派发/取锁")
        got_lock = False
        opened_ino = None
        opened_dev = None
        while not got_lock:
            if _shutdown:
                write("error", note="收到终止信号，未取锁即退出",
                      wait_reason=None)
                return 1

            maint = read_maintenance()
            if maint.get("active"):
                # 闸门关闭：不开新文件、不取锁，安静等待
                write("waiting", wait_reason="maintenance",
                      note="检修停派中，取锁被闸门拦截",
                      locked_ino=None, locked_dev=None, locked_path=None)
                time.sleep(config.LOCK_RETRY_INTERVAL)
                continue

            identity = file_identity(config.LOCK_PATH)
            if not identity["exists"]:
                write("error", note=f"锁文件不存在: {config.LOCK_PATH}",
                      wait_reason=None)
                return 1

            try:
                fd = os.open(config.LOCK_PATH, os.O_RDWR)
            except OSError as exc:
                write("error", note=f"打开锁文件失败: {exc}", wait_reason=None)
                return 1
            opened_ino = identity["inode"]
            opened_dev = identity["device"]

            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                # 别的任务正持锁：关掉本轮 fd 后重试，避免占着旧文件描述符
                os.close(fd)
                fd = None
                write("waiting", wait_reason="locked",
                      note=f"锁被持有，等待释放 (inode={opened_ino})",
                      locked_ino=None, locked_dev=None, locked_path=None)
                time.sleep(config.LOCK_RETRY_INTERVAL)
                continue

            # 拿到锁后的双重核验，消除“核验与取锁之间”的竞态
            maint = read_maintenance()
            if maint.get("active"):
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                fd = None
                write("waiting", wait_reason="maintenance",
                      note="取锁瞬间检修开始，立即释放并退回等待",
                      locked_ino=None, locked_dev=None, locked_path=None)
                time.sleep(config.LOCK_RETRY_INTERVAL)
                continue

            current = file_identity(config.LOCK_PATH)
            if not current["exists"] or current["inode"] != opened_ino:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                fd = None
                write("waiting", wait_reason="replaced",
                      note="取锁瞬间锁文件已被替换，按最新文件重新取锁",
                      locked_ino=None, locked_dev=None, locked_path=None)
                time.sleep(config.LOCK_RETRY_INTERVAL)
                continue

            got_lock = True

        # ---- 持锁阶段：模拟备份过程，真实 flock 一直持有 ---------------
        st = os.fstat(fd)
        write(
            "running",
            wait_reason=None,
            locked_ino=st.st_ino,
            locked_dev=st.st_dev,
            locked_path=config.LOCK_PATH,
            note=f"已持锁，模拟备份 {args.duration}s",
            started_at=_now(),
        )

        deadline = time.monotonic() + max(0.0, args.duration)
        while time.monotonic() < deadline:
            if _shutdown:
                write("error", note="收到终止信号，备份中断",
                      locked_ino=st.st_ino, locked_dev=st.st_dev,
                      locked_path=config.LOCK_PATH)
                return 1
            time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))

        write("done", wait_reason=None, locked_ino=st.st_ino,
              locked_dev=st.st_dev, locked_path=config.LOCK_PATH,
              note="备份完成，释放锁", finished_at=_now())
        return 0
    finally:
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(fd)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
