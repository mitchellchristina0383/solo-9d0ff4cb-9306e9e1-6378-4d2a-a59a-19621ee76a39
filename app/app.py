"""Flask 检修台：备份派发、flock 锁文件替换与停派/恢复核验。

进程模型（单容器单进程，waitress 提供多线程 HTTP，后台一个调度线程）：
  * 每个备份任务是 app.worker 子进程，自己对锁路径做真实 fcntl.flock；
  * 调度线程负责收割僵尸任务、按在途上限派发新任务、定时备份入队；
  * 检修状态落盘在 maintenance.json，worker 每轮取锁前都读它（闸门）；
  * “替换锁文件”用 os.replace 原子换路径，新旧 inode 不同，可复现
    “旧任务锁旧文件、新任务锁同名新文件”的双锁场景；
  * 恢复放行前按 *最新* 文件重新核验，任一步失败都保持停派并在网页展示原因。
"""
from __future__ import annotations

import fcntl
import os
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from . import config
from .common import (
    atomic_write_json,
    file_identity,
    parse_proc_locks,
    read_json,
    read_maintenance,
    write_maintenance,
)

TERMINAL_STATES = {"done", "error"}
LIVE_STATES = {"queued", "waiting", "running"}
PACKAGE_ROOT = Path(__file__).resolve().parent.parent
WORKER_LOG = os.path.join(config.DATA_DIR, "worker-stderr.log")


def now_str() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class Console:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.queue: deque[str] = deque()          # 待派发 job_id
        self.procs: dict[str, subprocess.Popen] = {}
        self.events: deque[dict] = deque(maxlen=200)
        self._job_seq = 0
        self._last_schedule = 0.0
        self._stop = threading.Event()
        self._ensure_lock_file()
        self._reconcile_on_startup()
        self._load_events()

    # ------------------------------------------------------------------ #
    # 持久化辅助
    # ------------------------------------------------------------------ #
    def _state_path(self, job_id: str) -> str:
        return os.path.join(config.STATE_DIR, f"{job_id}.json")

    def _load_job(self, job_id: str) -> dict | None:
        return read_json(self._state_path(job_id), None)

    def _all_jobs(self) -> list[dict]:
        jobs: list[dict] = []
        if os.path.isdir(config.STATE_DIR):
            for name in sorted(os.listdir(config.STATE_DIR)):
                if name.endswith(".json"):
                    data = read_json(os.path.join(config.STATE_DIR, name), None)
                    if isinstance(data, dict):
                        jobs.append(data)
        jobs.sort(key=lambda j: j.get("job_id", ""), reverse=True)
        return jobs[:100]

    def _history(self) -> list[dict]:
        data = read_json(config.HISTORY_PATH, [])
        return data if isinstance(data, list) else []

    def _append_history(self, entry: dict) -> list[dict]:
        history = self._history()
        history.append(entry)
        atomic_write_json(config.HISTORY_PATH, history)
        return history

    def log_event(self, kind: str, message: str) -> None:
        event = {"at": now_str(), "kind": kind, "message": message}
        self.events.append(event)
        try:
            with open(config.EVENTS_PATH, "a", encoding="utf-8") as fh:
                fh.write(f"{event['at']} [{kind}] {message}\n")
        except OSError:
            pass

    def _load_events(self) -> None:
        try:
            with open(config.EVENTS_PATH, "r", encoding="utf-8") as fh:
                lines = fh.readlines()[-200:]
        except FileNotFoundError:
            return
        for line in lines:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(" [", 1)
            if len(parts) == 2:
                at, rest = parts
                kind, _, message = rest.partition("] ")
                self.events.append({"at": at, "kind": kind, "message": message})

    # ------------------------------------------------------------------ #
    # 锁文件
    # ------------------------------------------------------------------ #
    def _ensure_lock_file(self) -> None:
        history = self._history()
        if not os.path.exists(config.LOCK_PATH) and not history:
            self._create_lock_file("初始化锁文件")
        elif not os.path.exists(config.LOCK_PATH):
            # 数据卷里有历史但文件缺失：按最新一代重建
            self._create_lock_file("启动时锁文件缺失，按新一代重建")

    def _create_lock_file(self, note: str) -> dict:
        """在锁目录生成新代文件并原子替换到当前锁路径。"""
        generation = f"gen-{uuid.uuid4().hex[:12]}"
        tmp_path = os.path.join(config.LOCK_DIR, f".lock.tmp-{os.getpid()}")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(f"backup lock generation: {generation}\ncreated_at: {now_str()}\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, config.LOCK_PATH)
        dir_fd = os.open(config.LOCK_DIR, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        ident = file_identity(config.LOCK_PATH)
        entry = {
            "seq": len(self._history()) + 1,
            "generation": generation,
            "path": config.LOCK_PATH,
            "inode": ident["inode"],
            "device": ident["device"],
            "replaced_at": now_str(),
            "note": note,
        }
        self._append_history(entry)
        return entry

    def replace_lock_file(self, note: str = "网页手动替换") -> dict:
        """任意时刻都允许换锁（检修中也可），换完要求重新核验。"""
        with self.lock:
            entry = self._create_lock_file(note)
            maint = read_maintenance()
            if maint.get("active"):
                maint["last_verify"] = {
                    "ok": False,
                    "at": None,
                    "reason": "锁文件已在检修期间替换，放行前必须按最新文件重新核验",
                }
                write_maintenance(maint)
            self.log_event("lock", f"锁文件已替换 -> {entry['generation']} "
                                   f"(inode={entry['inode']})；{note}")
            return entry

    # ------------------------------------------------------------------ #
    # 任务生命周期
    # ------------------------------------------------------------------ #
    def _reconcile_on_startup(self) -> None:
        """进程重启后：已派发但 PID 已死的任务判失败；queued 任务按序重新入队。"""
        requeue = []
        for job in self._all_jobs():
            state = job.get("state")
            if state == "queued" and not job.get("pid"):
                requeue.append(job["job_id"])
                continue
            if state in LIVE_STATES and not pid_alive(job.get("pid")):
                job["state"] = "error"
                job["note"] = (job.get("note") or "") + "；服务重启时进程已不存在"
                job["updated_at"] = now_str()
                atomic_write_json(self._state_path(job["job_id"]), job)
                self.log_event("job", f"{job['job_id']} 重启回收：进程不存在，标记失败")
        # _all_jobs 按 job_id 倒序返回，这里反转回 FIFO 入队顺序
        for job_id in reversed(requeue):
            if job_id not in self.queue:
                self.queue.append(job_id)
                self.log_event("job", f"{job_id} 重启后重新排队")

    def _reap_locked(self) -> None:
        for job_id, proc in list(self.procs.items()):
            rc = proc.poll()
            if rc is None:
                continue
            self.procs.pop(job_id, None)
            job = self._load_job(job_id)
            if job and job.get("state") in LIVE_STATES:
                job["state"] = "done" if rc == 0 else "error"
                job["note"] = (job.get("note") or "") + f"；进程退出码 {rc}"
                job["updated_at"] = now_str()
                atomic_write_json(self._state_path(job_id), job)
            self.log_event("job", f"{job_id} 进程结束 (exit={rc})")
        # 兜底：worker 已派发(pid 非空)但进程不在登记表里（如服务被 kill 重启）。
        # 注意 queued 任务 pid=None，属于尚未派发，绝不能在这里判死。
        for job in self._all_jobs():
            pid = job.get("pid")
            if (job.get("state") in LIVE_STATES and pid
                    and job["job_id"] not in self.procs
                    and not pid_alive(pid)):
                job["state"] = "error"
                job["note"] = (job.get("note") or "") + "；进程已消失"
                job["updated_at"] = now_str()
                atomic_write_json(self._state_path(job["job_id"]), job)

    def _spawn_locked(self, job_id: str, duration: float) -> None:
        log_fd = open(WORKER_LOG, "a", encoding="utf-8")
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "app.worker",
                "--job-id", job_id,
                "--duration", str(duration),
            ],
            cwd=str(PACKAGE_ROOT),
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=log_fd,
            close_fds=True,
        )
        self.procs[job_id] = proc
        self.log_event("dispatch", f"派发 {job_id} (pid={proc.pid}, 时长={duration}s)")

    def enqueue_backup(self, duration: float | None = None,
                       source: str = "手动") -> dict:
        with self.lock:
            self._job_seq += 1
            job_id = (f"job-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
                      f"-{self._job_seq:03d}")
            duration = float(config.DEFAULT_DURATION if duration is None else duration)
            atomic_write_json(self._state_path(job_id), {
                "job_id": job_id,
                "pid": None,
                "state": "queued",
                "duration": duration,
                "wait_reason": None,
                "locked_ino": None,
                "locked_dev": None,
                "locked_path": None,
                "note": f"{source}发起，排队等待调度",
                "updated_at": now_str(),
            })
            self.queue.append(job_id)
            self.log_event("job", f"{source}发起备份 {job_id}，持锁时长 {duration}s")
            return {"job_id": job_id, "duration": duration}

    # ------------------------------------------------------------------ #
    # 检修闸门与恢复核验
    # ------------------------------------------------------------------ #
    def start_maintenance(self, reason: str) -> dict:
        with self.lock:
            maint = read_maintenance()
            if maint.get("active"):
                return {"ok": False, "reason": "检修已在进行中"}
            write_maintenance({
                "active": True,
                "since": now_str(),
                "reason": reason or "网页手动开始检修",
                "last_verify": None,
            })
            self.log_event("maintenance",
                           f"开始检修，停止派发。原因：{reason or '网页手动操作'}")
            return {"ok": True}

    def verify_and_resume(self) -> dict:
        """放行前的完整核验。任何一步失败都保持停派并返回原因。"""
        with self.lock:
            self._reap_locked()
            maint = read_maintenance()
            if not maint.get("active"):
                return {"ok": True, "reason": "当前并非检修状态，无需放行"}

            def fail(reason: str) -> dict:
                maint["last_verify"] = {"ok": False, "at": now_str(), "reason": reason}
                write_maintenance(maint)
                self.log_event("verify", f"核验未通过，保持停派：{reason}")
                return {"ok": False, "reason": reason}

            # 1) 当前锁路径必须存在，且必须是历史中最新一代（防止换锁后又被动过）
            ident = file_identity(config.LOCK_PATH)
            if not ident["exists"]:
                return fail(f"当前锁文件不存在：{config.LOCK_PATH}")
            history = self._history()
            latest = history[-1] if history else None
            if not latest or latest.get("inode") != ident["inode"]:
                return fail("当前路径指向的文件与最新一代锁记录不一致，"
                            "疑似检修期间又被外部替换，请重新替换/核验")

            # 2) 不允许任何存活的持锁任务（旧锁必须全数释放）
            jobs = self._all_jobs()
            live = [j for j in jobs if j.get("state") in LIVE_STATES
                    and pid_alive(j.get("pid"))]
            holders = [j for j in live if j.get("state") == "running"]
            if holders:
                detail = ", ".join(
                    f"{j['job_id']}(pid={j['pid']},inode={j.get('locked_ino')})"
                    for j in holders)
                return fail(f"仍有持锁任务未结束：{detail}")

            # 3) 内核视角：与本服务相关的 FLOCK 必须全部消失
            #    既查本服务 worker PID，也查历史各代锁文件的 inode，
            #    覆盖“任务状态已结束但 fd 泄漏仍占着旧文件”的情况。
            live_pids = {j["pid"] for j in live if j.get("pid")}
            history_inodes = {h["inode"] for h in history}
            flocks = parse_proc_locks()
            relevant = [
                r for r in flocks
                if r["pid"] in live_pids or r["inode"] in history_inodes
            ]
            if relevant:
                detail = "; ".join(
                    f"pid={r['pid']} inode={r['inode']} ({r['mode']})"
                    for r in relevant)
                return fail(f"内核 /proc/locks 中仍存在相关 flock：{detail}")

            # 4) 对 *最新* 文件做一次非阻塞试探取锁，取锁瞬间再核身份
            try:
                fd = os.open(config.LOCK_PATH, os.O_RDWR)
            except OSError as exc:
                return fail(f"打开当前锁文件失败：{exc}")
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return fail("当前锁文件仍被其他进程排他锁定，无法取得锁")
                after = file_identity(config.LOCK_PATH)
                if not after["exists"] or after["inode"] != ident["inode"]:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return fail("试探取锁期间锁文件又被替换，已放弃本次放行")
                # 试探成功，立刻释放——这把锁不属于任何备份任务
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

            # 全部通过：恢复派发。等待中的 worker 下一轮轮询自行开跑
            write_maintenance({
                "active": False,
                "since": maint.get("since"),
                "reason": maint.get("reason"),
                "last_verify": {"ok": True, "at": now_str(),
                                "reason": "核验通过：旧锁全释放，当前最新文件可取锁"},
            })
            waiters = [j["job_id"] for j in live if j.get("state") == "waiting"]
            queued = list(self.queue)
            self.log_event(
                "maintenance",
                f"核验通过，恢复派发。等待中任务 {waiters or '无'}，"
                f"排队任务 {queued or '无'}")
            return {"ok": True, "reason": "核验通过，已恢复派发",
                    "waiting": waiters, "queued": queued}

    # ------------------------------------------------------------------ #
    # 后台调度线程
    # ------------------------------------------------------------------ #
    def run_forever(self) -> None:
        while not self._stop.wait(config.TICK_INTERVAL):
            try:
                self._tick()
            except Exception as exc:  # 后台线程不能因单次异常死掉
                self.log_event("error", f"调度循环异常：{exc!r}")

    def _tick(self) -> None:
        with self.lock:
            maint = read_maintenance()
            jobs = self._all_jobs()
            inflight = sum(1 for j in jobs
                           if j.get("state") in LIVE_STATES
                           and pid_alive(j.get("pid")))

            # 定时备份入队（检修期间只入队，worker 闸门会挡住取锁）
            if config.SCHEDULE_INTERVAL > 0:
                now = time.monotonic()
                if self._last_schedule == 0.0 or \
                        now - self._last_schedule >= config.SCHEDULE_INTERVAL:
                    self._last_schedule = now
                    self.enqueue_backup(source="定时")

            # 先按闸门决定是否派发，再收割已退出进程。
            # 反过来的话，刚被 verify_and_resume 同步收割过的周期里，
            # 队列任务要白白多等一个 tick。
            if not maint.get("active"):
                while self.queue and inflight < config.MAX_INFLIGHT:
                    job_id = self.queue.popleft()
                    job = self._load_job(job_id)
                    if not job:
                        continue
                    self._spawn_locked(job_id, float(job["duration"]))
                    inflight += 1
            self._reap_locked()

    # ------------------------------------------------------------------ #
    # 页面快照
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        with self.lock:
            self._reap_locked()
            maint = read_maintenance()
            ident = file_identity(config.LOCK_PATH)
            history = self._history()
            jobs = self._all_jobs()
            flocks = parse_proc_locks()

            pid_to_job = {j["pid"]: j for j in jobs if j.get("pid")}
            inode_to_gen = {h["inode"]: h["generation"] for h in history}
            current_ino = ident["inode"]
            for row in flocks:
                job = pid_to_job.get(row["pid"])
                row["job_id"] = job.get("job_id") if job else None
                row["generation"] = inode_to_gen.get(row["inode"])
                row["is_current_lock"] = (row["inode"] == current_ino)

            inflight = sum(1 for j in jobs
                           if j.get("state") in LIVE_STATES
                           and pid_alive(j.get("pid")))
            return {
                "now": now_str(),
                "maintenance": maint,
                "lock": {
                    "path": config.LOCK_PATH,
                    **ident,
                    "generation": next(
                        (h["generation"] for h in reversed(history)
                         if h["inode"] == ident["inode"]), None),
                },
                "scheduler": {
                    "interval": config.SCHEDULE_INTERVAL,
                    "enabled": config.SCHEDULE_INTERVAL > 0,
                    "queued": list(self.queue),
                },
                "defaults": {
                    "duration": config.DEFAULT_DURATION,
                    "max_inflight": config.MAX_INFLIGHT,
                },
                "inflight": inflight,
                "jobs": jobs,
                "proc_locks": flocks,
                "history": list(reversed(history)),
                "events": list(self.events)[-40:][::-1],
            }


console = Console()
app = Flask(__name__)


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/snapshot")
def api_snapshot():
    return jsonify(console.snapshot())


@app.post("/api/backup")
def api_backup():
    payload = request.get_json(silent=True) or request.form
    raw = payload.get("duration")
    try:
        duration = float(raw) if raw not in (None, "") else config.DEFAULT_DURATION
        if duration <= 0 or duration > 3600:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"ok": False, "reason": "时长需为 0~3600 秒之间的数字"}), 400
    result = console.enqueue_backup(duration)
    return jsonify({"ok": True, **result})


@app.post("/api/maintenance/start")
def api_maint_start():
    payload = request.get_json(silent=True) or request.form
    return jsonify(console.start_maintenance(str(payload.get("reason") or "")))


@app.post("/api/lock/replace")
def api_lock_replace():
    payload = request.get_json(silent=True) or request.form
    entry = console.replace_lock_file(str(payload.get("note") or "网页手动替换"))
    return jsonify({"ok": True, **entry})


@app.post("/api/maintenance/resume")
def api_maint_resume():
    return jsonify(console.verify_and_resume())


def main() -> None:
    from waitress import serve

    thread = threading.Thread(target=console.run_forever, name="dispatcher",
                              daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", "8080"))
    serve(app, host="0.0.0.0", port=port, threads=8)


if __name__ == "__main__":
    main()
