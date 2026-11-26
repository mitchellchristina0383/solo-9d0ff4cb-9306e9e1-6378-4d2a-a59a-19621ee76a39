"""样例备份任务（独立子进程）。

行为模拟一个定时备份作业：
  1. 在任务状态文件中登记 queued -> waiting；
  2. 对 *当前* 锁文件用 fcntl.flock(LOCK_EX) 做真实的非阻塞轮询排他锁；
  3. 拿到锁后置 running；普通样例按设定时长持锁等待；
     压缩样例（--compression-sample）则 *顺序* 启动两个
     ``app.compress_child`` 子进程真实压缩本地数据，并把压缩 CPU
     （用户态 + 内核态）实时/终值计量进本任务状态；
  4. 结束后释放锁并置为 done / error。

CPU 计量的正确性（详见 app/compression.py）：
  * 运行中的子进程读 /proc/<pid>/stat 的 utime+stime 做实时展示；
  * 子进程退出时用 os.wait4 取 rusage 终值并只提交一次——即使它在
    网页两次刷新之间跑完，用量也不会漏；
  * committed 终值 + 活动子进程采样的模型保证交接时总数只增不减、
    每份用量只计一次；状态原子落盘，意外中断/容器重启后数值仍在。

关键锁细节：每一轮取锁都重新 open() 当前锁路径，并在拿到锁之后再次核对
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
import subprocess
import sys
import time
from datetime import datetime, timezone

from . import config
from .common import atomic_write_json, file_identity, read_json, read_maintenance
from .compression import CompressionMeter

_shutdown = False

# 让压缩子进程在其父 worker 意外被 SIGKILL（未走进程组/容器 cgroup 回收）时
# 也能自动收到 SIGTERM，避免孤儿压缩进程继续烧 CPU。仅 Linux/glibc 可用，
# 取不到 libc 时退化为原来的行为（网页中断走 killpg，容器重启走 cgroup）。
try:  # pragma: no cover - 与运行平台强相关
    import ctypes

    _libc = ctypes.CDLL("libc.so.6", use_errno=True)
except Exception:  # pragma: no cover
    _libc = None


def _compression_child_preexec() -> None:
    try:
        if _libc is not None:
            _libc.prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG = 1
    except BaseException:
        pass


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    _shutdown = True


def main() -> int:
    parser = argparse.ArgumentParser(description="样例备份 worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--compression-sample", action="store_true")
    parser.add_argument("--compress-profile", default="standard")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    state_path = os.path.join(config.STATE_DIR, f"{args.job_id}.json")
    meter = CompressionMeter(args.compress_profile) if args.compression_sample else None

    def write(state: str, **extra) -> None:
        payload = {
            "job_id": args.job_id,
            "pid": os.getpid(),
            "state": state,
            "duration": args.duration,
            "compression_sample": bool(args.compression_sample),
            "updated_at": _now(),
            **extra,
        }
        if meter is not None:
            payload["compression"] = meter.to_dict()
            payload.update(meter.top_level())
        atomic_write_json(state_path, payload)

    write("queued", wait_reason=None, locked_ino=None, locked_dev=None,
          locked_path=None,
          note=("压缩样例已登记，等待派发闸门" if args.compression_sample
                else "任务已登记，等待派发闸门"))

    fd: int | None = None
    lock_meta: dict = {}
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

        # ---- 持锁阶段：记录锁身份 -------------------------------------
        st = os.fstat(fd)
        lock_meta = {
            "locked_ino": st.st_ino,
            "locked_dev": st.st_dev,
            "locked_path": config.LOCK_PATH,
        }
        write(
            "running",
            wait_reason=None,
            **lock_meta,
            note=("已持锁，压缩样例：准备顺序启动两个压缩子进程"
                  if args.compression_sample
                  else f"已持锁，模拟备份 {args.duration}s"),
            started_at=_now(),
        )

        if args.compression_sample and meter is not None:
            rc = _run_compression(args, meter, write, lock_meta)
            if rc != 0:
                return rc
            write("done", wait_reason=None, **lock_meta,
                  note=("压缩样例完成：两个子进程用量均已计入，锁释放"
                        if meter.complete else
                        "压缩样例结束，但用量未计全"),
                  finished_at=_now())
            return 0

        # ---- 普通样例：持锁运行指定时长 --------------------------------
        deadline = time.monotonic() + max(0.0, args.duration)
        while time.monotonic() < deadline:
            if _shutdown:
                write("error", note="收到终止信号，备份中断",
                      **lock_meta)
                return 1
            time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))

        write("done", wait_reason=None, **lock_meta,
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


def _launch_compress_child(args: argparse.Namespace, index: int
                           ) -> subprocess.Popen:
    """启动第 index 个压缩子进程。

    close_fds=True 保证 flock fd 不会被 exec 后的子进程继承；
    不另开会话/进程组，子进程与 worker 同进程组，检修台“中断”
    发 killpg 时能一并终止，避免留下孤儿压缩进程。
    """
    return subprocess.Popen(
        [
            sys.executable, "-m", "app.compress_child",
            "--job-id", args.job_id,
            "--index", str(index),
            "--profile", args.compress_profile,
        ],
        cwd=_package_root(),
        stdin=subprocess.DEVNULL,
        close_fds=True,
        preexec_fn=_compression_child_preexec if _libc is not None else None,
    )


def _package_root() -> str:
    # app/worker.py -> 上两级即包根（容器内 /srv，宿主机为仓库根）
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _terminate_and_reap(proc: subprocess.Popen):
    """终止压缩子进程并用 *唯一* 一次 wait4 回收，返回 rusage。

    不调用 Popen.wait/poll：本模块统一用 os.wait4 回收以拿到 rusage，
    避免 waitpid/wait4 双重回收导致其中一方拿不到账。
    """
    pid = proc.pid
    for sig, grace in ((signal.SIGTERM, 2.0), (signal.SIGKILL, 2.0)):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            try:
                wpid, _status, rusage = os.wait4(pid, os.WNOHANG)
            except ChildProcessError:
                return None
            if wpid == pid:
                return rusage
            time.sleep(0.05)
    try:
        _wpid, _status, rusage = os.wait4(pid, 0)
        return rusage
    except ChildProcessError:
        return None


def _run_compression(args: argparse.Namespace, meter: CompressionMeter,
                     write, lock_meta: dict) -> int:
    """顺序跑两个压缩子进程并完成计量。返回 worker 退出码。"""
    for index in (1, 2):
        if _shutdown:
            meter.mark_interrupted("启动下一个子进程前收到终止信号")
            write("error", wait_reason=None, **lock_meta,
                  note="压缩样例被终止，保留最后用量（未计全）")
            return 1

        proc = _launch_compress_child(args, index)
        meter.start_child(index, proc.pid)
        write("running", wait_reason=None, **lock_meta,
              note=f"压缩子进程 {index}/2 运行中 (pid={proc.pid})")

        reaped = False
        child_rc = 0
        while not reaped:
            # 非阻塞回收：子进程若已退出，立刻拿到 rusage 终值。
            # 即使它在网页两次刷新之间跑完，这里也会把账提交进原任务。
            try:
                wpid, status, rusage = os.wait4(proc.pid, os.WNOHANG)
            except ChildProcessError:
                wpid = proc.pid
                status = 0
                rusage = None
            if wpid == proc.pid:
                child_rc = os.waitstatus_to_exitcode(status) if status else 0
                if rusage is not None:
                    meter.account_reaped(proc.pid, rusage)
                else:
                    # 极端情况下拿不到 rusage：至少保证账目不丢进程，
                    # 用最后一次 /proc 采样作为该子进程终值。
                    child = meter.children[index - 1]
                    if not child.accounted:
                        class _RU:
                            ru_utime = child.live_user
                            ru_stime = child.live_system
                        meter.account_reaped(proc.pid, _RU())  # type: ignore[arg-type]
                reaped = True
                break

            meter.sample_active()
            write("running", wait_reason=None, **lock_meta,
                  note=f"压缩子进程 {index}/2 运行中 (pid={proc.pid})，"
                       f"CPU 用户+内核 实时累计")

            if _shutdown:
                rusage = _terminate_and_reap(proc)
                if rusage is not None:
                    meter.account_reaped(proc.pid, rusage)
                # 若 rusage 为 None（已被别处回收），活动子进程最后一次
                # /proc 采样仍由 totals() 计入，用量不会回落。
                meter.mark_interrupted("压缩过程中收到终止信号")
                write("error", wait_reason=None, **lock_meta,
                      note="压缩样例被中断：最后已记录用量已保留，标记为未计全")
                return 1

            time.sleep(config.COMPRESS_POLL_INTERVAL)

        if child_rc != 0:
            meter.mark_interrupted(f"压缩子进程 {index} 异常退出 rc={child_rc}")
            write("error", wait_reason=None, **lock_meta,
                  note=f"压缩子进程 {index}/2 异常退出(rc={child_rc})，"
                       f"已保留用量并标记未计全")
            return 1

        # 交接点立刻落盘一次“已提交终值”，总数只增不减
        write("running", wait_reason=None, **lock_meta,
              note=(f"压缩子进程 {index}/2 用量已计入"
                    + ("，继续启动下一个" if index == 1 else "，两个子进程完成")))

    meter.mark_complete()
    return 0


if __name__ == "__main__":
    sys.exit(main())
