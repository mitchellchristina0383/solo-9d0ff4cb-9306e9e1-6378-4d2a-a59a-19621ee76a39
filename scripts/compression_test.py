#!/usr/bin/env python3
"""压缩 CPU 样例的端到端计量测试（与容器内同一份代码）。

覆盖需求中的硬指标：
  1. 发起表单/首页包含“压缩样例”入口与压缩 CPU 列；
  2. 压缩任务的 CPU 从 0 开始，运行中可见且单调增长；
  3. quick 档子进程远快于网页 2s 刷新，退出后用量仍计入原任务：
     两个子进程各由 wait4 rusage 提交一次，总数=两子进程之和；
  4. 换锁后并行的两个压缩任务 PID/子进程/账目互不重叠（各算各的）；
  5. 网页“中断”(SIGTERM) 与 SIGKILL 强杀都保留最后用量并标“未计全”；
  6. 重启服务（等价容器重启，同一 BM_DATA_DIR 卷）后，计全/未计全
     与 CPU 数值仍可经 /api/snapshot 查到；启动回收会把 PID 已死的
     running 压缩任务判为 error/incomplete 且不清零。

用法：
  scripts/compression_test.py            # 自建临时数据目录与 8092 端口服务
  scripts/compression_test.py BASE_URL   # 对已运行服务跑前 5 组（不含重启）
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent


class Fail(Exception):
    pass


def must(cond: bool, msg: str) -> None:
    if not cond:
        raise Fail(msg)


def wait_for(predicate, desc: str, timeout: float = 45.0, interval: float = 0.2):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise Fail(f"等待超时：{desc}（最后={json.dumps(last, ensure_ascii=False)[:300]}）")


class Harness:
    def __init__(self, base: str, data_dir: str | None):
        self.base = base.rstrip("/")
        self.data_dir = data_dir

    def snap(self) -> dict:
        return requests.get(f"{self.base}/api/snapshot", timeout=5).json()

    def post(self, path: str, **body) -> dict:
        return requests.post(f"{self.base}{path}", json=body, timeout=15).json()

    def state_file(self, job_id: str) -> dict:
        if not self.data_dir:
            raise Fail("本用例需要自建服务与数据目录")
        with open(os.path.join(self.data_dir, "tasks", f"{job_id}.json"),
                  encoding="utf-8") as fh:
            return json.load(fh)

    def wait_state(self, job_id: str, state: str, timeout: float = 45) -> dict:
        return wait_for(
            lambda: self.state_file(job_id)
            if self.state_file(job_id)["state"] == state else None,
            f"{job_id} -> {state}", timeout)

    def wait_cpu_positive(self, job_id: str, threshold: float = 0.0) -> dict:
        def pred():
            j = self.state_file(job_id)
            c = j.get("compression") or {}
            if j["state"] == "running" and c.get("cpu_seconds", 0) > threshold:
                return j
            return None
        return wait_for(pred, f"{job_id} running + cpu>{threshold}", 45)


def start_server() -> tuple[subprocess.Popen, str, str]:
    data = tempfile.mkdtemp(prefix="bm-compress-")
    env = dict(os.environ, BM_DATA_DIR=data, BM_DEFAULT_DURATION="8",
               BM_SCHEDULE_INTERVAL="0", PORT="8092", PYTHONUNBUFFERED="1",
               BM_TICK_INTERVAL="0.1", BM_LOCK_RETRY_INTERVAL="0.1")
    py = str(ROOT / ".venv" / "bin" / "python")
    if not os.path.exists(py):
        py = sys.executable
    log = open(os.path.join(data, "server.log"), "w", encoding="utf-8")
    proc = subprocess.Popen([py, "-m", "app.app"], cwd=str(ROOT), env=env,
                            stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True)
    base = "http://127.0.0.1:8092"
    for _ in range(60):
        try:
            requests.get(f"{base}/api/snapshot", timeout=1)
            return proc, base, data
        except requests.RequestException:
            time.sleep(0.5)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except OSError:
        proc.kill()
    raise Fail("压缩测试服务启动超时，见数据目录 server.log")


def assert_accounting_exact(job: dict) -> dict:
    c = job["compression"]
    must(c["complete"] is True, f"{job['job_id']} compression 应 complete")
    must(c["status"] == "complete", "status 应为 complete")
    must(c["children_accounted"] == 2, "两个子进程都应 accounted")
    must(len(c["children"]) == 2, "应有两条 child 账")
    total_children = 0.0
    for ch in c["children"]:
        must(ch["accounted"] is True and ch["counted_once"] is True,
             f"子进程账必须标记只计一次：{ch}")
        must(str(ch.get("source", "")).startswith("wait4"),
             f"终值必须来自 wait4 rusage：{ch.get('source')}")
        total_children += float(ch["cpu_seconds"])
    must(abs(total_children - c["cpu_seconds"]) < 2e-6,
         f"总数 {c['cpu_seconds']} 必须等于两子进程之和 {total_children}")
    must(abs(c["committed_cpu_seconds"] - c["cpu_seconds"]) < 2e-6,
         "committed 必须等于总数")
    must(c["cpu_seconds"] > 0, "压缩 CPU 必须大于 0")
    return c


def test_page_controls(h: Harness) -> None:
    print("\n=== C1：首页包含压缩样例选项与压缩 CPU 列 ===")
    html = requests.get(f"{h.base}/", timeout=5).text
    for token in ("id=\"compression\"", "compressProfile", "压缩样例",
                  "压缩 CPU", "interruptJob", "快速退出"):
        must(token in html, f"首页缺少 {token}")
    print("首页含勾选框/负载档/压缩 CPU 列/中断操作：PASS")


def test_quick_monotonic_exact(h: Harness) -> str:
    print("\n=== C2：quick 子进程刷新间隙退出，仍精确计入且单调 ===")
    jid = h.post("/api/backup", compression=True, profile="quick")["job_id"]
    first = h.state_file(jid)
    must(first["compression"]["cpu_seconds"] == 0.0, "入队时压缩 CPU 必须为 0")
    must(first["compression_complete"] is False, "入队时不得标记计全")

    vals: list[float] = []
    states: list[str] = []
    t0 = time.monotonic()
    final = None
    while time.monotonic() - t0 < 30:
        j = h.state_file(jid)
        vals.append(float(j["compression"]["cpu_seconds"]))
        states.append(j["state"])
        if j["state"] in ("done", "error"):
            final = j
            break
        time.sleep(0.01)
    must(final is not None, "quick 任务应进入终态")
    elapsed = time.monotonic() - t0
    drops = [(vals[i], vals[i + 1]) for i in range(len(vals) - 1)
             if vals[i + 1] + 1e-9 < vals[i]]
    must(not drops, f"CPU 总数必须单调不减，出现回落 {drops[:3]}")
    must(final["state"] == "done", f"quick 应正常 done：{final['state']}")
    c = assert_accounting_exact(final)
    print(f"采样 {len(vals)} 次，全程回落 0；终值 {c['cpu_seconds']:.4f}s "
          f"(u={c['user_seconds']:.4f}+s={c['system_seconds']:.4f})，"
          f"两个子进程各计一次；观测窗口 {elapsed:.2f}s（网页刷新 2s）")
    print("C2 PASS")
    return jid


def test_live_growth(h: Harness) -> str:
    print("\n=== C3：standard 运行中可查看且实时增长 ===")
    jid = h.post("/api/backup", compression=True, profile="standard")["job_id"]
    h.wait_cpu_positive(jid, 0.0)
    j1 = h.state_file(jid)
    time.sleep(0.7)
    j2 = h.state_file(jid)
    must(j2["compression"]["cpu_seconds"] > j1["compression"]["cpu_seconds"],
         "运行中压缩 CPU 应增长")
    must(j2["compression"]["status"] == "running", "运行中 status=running")
    print(f"实时增长 {j1['compression']['cpu_seconds']:.3f} -> "
          f"{j2['compression']['cpu_seconds']:.3f}（state=running）")
    h.wait_state(jid, "done")
    assert_accounting_exact(h.state_file(jid))
    print("C3 PASS")
    return jid


def test_lock_swap_parallel(h: Harness) -> None:
    print("\n=== C4：换锁后并行样例各算各的 ===")
    ino0 = h.snap()["lock"]["inode"]
    a = h.post("/api/backup", compression=True, profile="standard")["job_id"]
    h.wait_cpu_positive(a)
    must(h.state_file(a)["locked_ino"] == ino0, "A 应持初始 inode")

    ino1 = h.post("/api/lock/replace", note="压缩测试换锁")["inode"]
    must(ino1 != ino0, "换锁 inode 必须变化")
    b = h.post("/api/backup", compression=True, profile="standard")["job_id"]
    h.wait_cpu_positive(b)
    ja, jb = h.state_file(a), h.state_file(b)
    must(ja["state"] == "running", "换锁后 A 应仍在旧 inode 上运行")
    must(jb["locked_ino"] == ino1, "B 必须锁新 inode 并与 A 并行")

    pids_a, pids_b = set(), set()
    for _ in range(5):
        for ch in h.state_file(a)["compression"]["children"]:
            if ch["pid"]:
                pids_a.add(ch["pid"])
        for ch in h.state_file(b)["compression"]["children"]:
            if ch["pid"]:
                pids_b.add(ch["pid"])
        time.sleep(0.3)
    must(not (pids_a & pids_b), f"两任务子进程 PID 不得重叠：{pids_a & pids_b}")

    h.wait_state(a, "done")
    h.wait_state(b, "done")
    ja_final, jb_final = h.state_file(a), h.state_file(b)
    ca = assert_accounting_exact(ja_final)
    cb = assert_accounting_exact(jb_final)
    final_pids_a = {ch["pid"] for ch in ca["children"]}
    final_pids_b = {ch["pid"] for ch in cb["children"]}
    must(not (final_pids_a & final_pids_b),
         f"两个任务的全部子进程 PID 不得重叠：{final_pids_a & final_pids_b}")
    print(f"A={a} cpu={ca['cpu_seconds']:.3f} child_pids={sorted(final_pids_a)}")
    print(f"B={b} cpu={cb['cpu_seconds']:.3f} child_pids={sorted(final_pids_b)}")
    print("PID/账目完全隔离：C4 PASS")


def test_interrupts(h: Harness) -> tuple[str, str]:
    print("\n=== C5：SIGTERM 中断 与 SIGKILL 强杀均保留用量、标未计全 ===")
    # --- 网页“中断”按钮走的 SIGTERM ---
    t = h.post("/api/backup", compression=True, profile="standard")["job_id"]
    j = h.wait_cpu_positive(t, 0.5)
    before = h.state_file(t)["compression"]["cpu_seconds"]
    r = h.post(f"/api/job/{t}/interrupt")
    must(r.get("ok"), f"中断接口应成功：{r}")
    h.wait_state(t, "error", timeout=20)
    c = h.state_file(t)["compression"]
    must(c["complete"] is False and c["status"] == "incomplete",
         "SIGTERM 后必须 incomplete")
    must(c["count_label"] == "未计全", "必须显示未计全")
    must(c["cpu_seconds"] + 1e-9 >= before and c["cpu_seconds"] > 0,
         "用量必须保留且不回落")
    print(f"SIGTERM {t}: {before:.3f} -> {c['cpu_seconds']:.3f}s，未计全，"
          f"已计子进程 {c['children_accounted']}/2")

    # --- SIGKILL 整个 worker 进程组（模拟意外硬中断）---
    k = h.post("/api/backup", compression=True, profile="standard")["job_id"]
    jk = h.wait_cpu_positive(k, 0.5)
    pid = jk["pid"]
    before_k = h.state_file(k)["compression"]["cpu_seconds"]
    try:
        os.killpg(os.getpgid(pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    wait_for(lambda: h.state_file(k)["state"] == "error",
             "SIGKILL 后收割为 error", 20)
    ck = h.state_file(k)["compression"]
    must(ck["complete"] is False and ck["status"] == "incomplete",
         "SIGKILL 后必须 incomplete")
    must(abs(ck["cpu_seconds"] - before_k) < 1e-6 and ck["cpu_seconds"] > 0,
         "SIGKILL 来不及补账，必须原样保留最后一次落盘值")
    print(f"SIGKILL {k}: 原样保留 {ck['cpu_seconds']:.3f}s，未计全")
    print("C5 PASS")
    return t, k


def test_restart_persistence(data: str, done_id: str,
                             incomplete_ids: list[str]) -> None:
    print("\n=== C6：重启服务（同一数据卷）后用量与是否计全仍可查 ===")
    # 伪造一条 PID 已不存在的 running 压缩任务，确定性验证启动回收
    stale_id = "job-stale-restart-check"
    atomic = {
        "job_id": stale_id, "pid": 999999, "state": "running", "duration": 9,
        "compression_sample": True, "note": "stale",
        "updated_at": "2026-11-26T05:00:00+08:00",
        "compression": {
            "sample": True, "profile": "standard", "status": "running",
            "complete": False, "children_total": 2, "children_accounted": 1,
            "cpu_seconds": 2.345, "user_seconds": 2.1, "system_seconds": 0.245,
        },
    }
    with open(os.path.join(data, "tasks", f"{stale_id}.json"), "w",
              encoding="utf-8") as fh:
        json.dump(atomic, fh)

    proc2, base2, _ = start_server_on(data, 8093)
    try:
        h2 = Harness(base2, data)
        wait_for(lambda: h2.snap() if h2.snap().get("jobs") else None,
                 "重启后快照可用", 30)
        j = h2.state_file(stale_id)
        c = j["compression"]
        must(j["state"] == "error", "启动回收应把死 PID 任务判 error")
        must(c["complete"] is False and c["status"] == "incomplete",
             "stale 压缩任务应标未计全")
        must(c["cpu_seconds"] == 2.345, "回收不得清零/改写最后用量")
        print(f"启动回收 {stale_id}: error/未计全，cpu 仍为 {c['cpu_seconds']}")

        d = h2.state_file(done_id)["compression"]
        must(d["complete"] is True and d["cpu_seconds"] > 0,
             "done 任务重启后仍应计全且数值保留")
        print(f"已计全任务 {done_id}: 重启后 complete，cpu={d['cpu_seconds']:.4f}")
        for jid in incomplete_ids:
            c = h2.state_file(jid)["compression"]
            must(c["complete"] is False and c["cpu_seconds"] > 0,
                 f"{jid} 未计全状态与用量必须跨重启保留")
            row = next((x for x in h2.snap()["jobs"] if x["job_id"] == jid), None)
            must(row is not None and row["compression"]["count_label"] == "未计全",
                 "快照/页面必须能读到“未计全”")
            print(f"未计全任务 {jid}: 重启后仍可查，cpu={c['cpu_seconds']:.3f}")
        print("C6 PASS")
    finally:
        try:
            os.killpg(proc2.pid, signal.SIGTERM)
        except OSError:
            proc2.terminate()


def start_server_on(data: str, port: int):
    env = dict(os.environ, BM_DATA_DIR=data, BM_DEFAULT_DURATION="8",
               BM_SCHEDULE_INTERVAL="0", PORT=str(port), PYTHONUNBUFFERED="1",
               BM_TICK_INTERVAL="0.1", BM_LOCK_RETRY_INTERVAL="0.1")
    py = str(ROOT / ".venv" / "bin" / "python")
    if not os.path.exists(py):
        py = sys.executable
    log = open(os.path.join(data, f"server-{port}.log"), "a", encoding="utf-8")
    proc = subprocess.Popen([py, "-m", "app.app"], cwd=str(ROOT), env=env,
                            stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True)
    base = f"http://127.0.0.1:{port}"
    for _ in range(60):
        try:
            requests.get(f"{base}/api/snapshot", timeout=1)
            return proc, base, data
        except requests.RequestException:
            time.sleep(0.5)
    raise Fail("重启服务启动超时")


def main() -> int:
    external = len(sys.argv) > 1
    proc = None
    data = None
    if external:
        h = Harness(sys.argv[1], os.environ.get("BM_DATA_DIR"))
    else:
        proc, base, data = start_server()
        h = Harness(base, data)
    try:
        test_page_controls(h)
        done_id = test_quick_monotonic_exact(h)
        test_live_growth(h)
        test_lock_swap_parallel(h)
        t, k = test_interrupts(h)
        if not external:
            # 等价“停容器”：停掉首个服务进程，再用同一 BM_DATA_DIR 重启
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
            proc = None
            test_restart_persistence(data, done_id, [t, k])
    except Fail as exc:
        print(f"\n❌ 压缩测试失败：{exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"\n❌ 压缩测试异常：{exc!r}", file=sys.stderr)
        return 2
    finally:
        if proc is not None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait(timeout=10)
            except Exception:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    pass
        shutil.rmtree(data, ignore_errors=True)
    print("\n🎉 压缩计量全部场景通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
