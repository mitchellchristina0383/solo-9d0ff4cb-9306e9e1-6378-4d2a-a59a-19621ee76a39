#!/usr/bin/env python3
"""端到端冒烟测试（在宿主机 venv 中跑，与容器内同一份代码）。

覆盖需求中明确要求实测的两件事：
  A. 锁文件被替换后，旧任务仍持有旧 inode 的 flock，新任务持有新 inode，
     /proc/locks 中同时出现两把 FLOCK；
  B. 检修停派后等待中的任务不会抢跑；检修期间再次换锁，放行前必须按最新
     文件重新核验（锁文件被外部改动时核验会被拒并保持停派）。
另外覆盖：不杀任务、持锁者自行结束后才放行；恢复后任务锁的是最新文件。

用法： scripts/smoke_test.py [BASE_URL]
不带参数时自动用临时数据目录拉起一个服务实例，测完自动清理。
"""
from __future__ import annotations

import json
import os
import shutil
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


def wait_for(predicate, desc: str, timeout: float = 30.0,
             interval: float = 0.3):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise Fail(f"等待超时：{desc}（最后状态={json.dumps(last, ensure_ascii=False)[:400]}")


class Harness:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def snap(self) -> dict:
        return requests.get(f"{self.base}/api/snapshot", timeout=5).json()

    def post(self, path: str, **body) -> dict:
        return requests.post(f"{self.base}{path}", json=body, timeout=10).json()

    def job(self, job_id: str) -> dict:
        # 直接读该任务的状态文件，不受快照只保留最近 100 条的影响
        data_dir = os.environ.get("BM_DATA_DIR")
        if data_dir:
            path = os.path.join(data_dir, "tasks", f"{job_id}.json")
            try:
                with open(path, encoding="utf-8") as fh:
                    return json.load(fh)
            except FileNotFoundError:
                pass
        for j in self.snap()["jobs"]:
            if j["job_id"] == job_id:
                return j
        raise Fail(f"任务 {job_id} 不在快照中")

    def wait_job(self, job_id: str, state: str, timeout: float = 30.0) -> dict:
        def pred():
            j = self.job(job_id)
            return j if j["state"] == state else None
        try:
            return wait_for(pred, f"{job_id} -> {state}", timeout)
        except Fail:
            # 超时时把实际状态带出来，便于定位是状态机问题还是时序问题
            actual = self.job(job_id)
            raise Fail(f"{job_id} 未在 {timeout}s 内进入 {state}，"
                       f"实际 state={actual.get('state')} "
                       f"wait_reason={actual.get('wait_reason')} "
                       f"pid={actual.get('pid')} note={actual.get('note')}")


def start_server() -> tuple[subprocess.Popen, str, str]:
    data = tempfile.mkdtemp(prefix="bm-smoke-")
    env = dict(os.environ, BM_DATA_DIR=data, BM_DEFAULT_DURATION="8",
               BM_SCHEDULE_INTERVAL="0", PORT="8091", PYTHONUNBUFFERED="1",
               BM_TICK_INTERVAL="0.1", BM_LOCK_RETRY_INTERVAL="0.1")
    venv_py = str(ROOT / ".venv" / "bin" / "python")
    py = venv_py if os.path.exists(venv_py) else sys.executable
    log = open(os.path.join(data, "server.log"), "w", encoding="utf-8")
    proc = subprocess.Popen([py, "-m", "app.app"], cwd=str(ROOT),
                            env=env, stdout=log, stderr=subprocess.STDOUT)
    base = "http://127.0.0.1:8091"
    for _ in range(60):
        try:
            requests.get(f"{base}/api/snapshot", timeout=1)
            return proc, base, data
        except requests.RequestException:
            time.sleep(0.5)
    proc.kill()
    raise Fail("服务启动超时，见数据目录 server.log")


# --------------------------------------------------------------------------- #
def test_page_render(h: Harness) -> None:
    print("\n=== 场景 C：网页可访问 ===")
    r = requests.get(f"{h.base}/", timeout=5)
    must(r.status_code == 200 and "检修台" in r.text, "首页应正常渲染")
    print("首页 HTTP 200 且包含检修台标题：PASS")


def test_dual_locks(h: Harness) -> None:
    print("\n=== 场景 A：两份锁同时被持有（换锁后新旧 inode 各持一把） ===")
    s = h.snap()
    old_ino = s["lock"]["inode"]
    print(f"初始锁 inode={old_ino} 代次={s['lock']['generation']}")

    j1 = h.post("/api/backup", duration=12)["job_id"]
    h.wait_job(j1, "running", timeout=10)
    holder1 = h.job(j1)
    must(holder1["locked_ino"] == old_ino, "j1 应持有初始文件的锁")
    print(f"[1] {j1} 已持旧锁 inode={holder1['locked_ino']}")

    # j2 在 j1 持锁期间发起：应处于 waiting（被同一把 flock 挡住）
    j2 = h.post("/api/backup", duration=8)["job_id"]
    h.wait_job(j2, "waiting", timeout=10)
    print(f"[2] {j2} 排队等锁，wait_reason={h.job(j2)['wait_reason']}")

    # 换锁：j1 继续锁旧文件（路径已不指向它，但 fd 上的 flock 仍有效）
    rep = h.post("/api/lock/replace", note="场景A换锁")
    new_ino = rep["inode"]
    must(new_ino != old_ino, "替换后 inode 必须变化")
    print(f"[3] 锁文件已替换，新 inode={new_ino} 代次={rep['generation']}")

    def two_flocks():
        snap = h.snap()
        inodes = {r["inode"] for r in snap["proc_locks"]}
        return snap if {old_ino, new_ino}.issubset(inodes) else None

    snap = wait_for(two_flocks, "/proc/locks 同时出现新旧两把 FLOCK", timeout=15)
    rows = {r["inode"]: r for r in snap["proc_locks"]}
    must(rows[old_ino]["job_id"] == j1, "旧 inode 的锁应属于 j1")
    must(rows[new_ino]["is_current_lock"] is True, "新 inode 应标注为当前文件")
    must(rows[old_ino]["is_current_lock"] is False, "旧 inode 应标注为非当前文件")
    print(f"[4] /proc/locks 双锁并存：j1->旧({old_ino})，"
          f"{rows[new_ino]['job_id']}->新({new_ino})")

    # j2 拿到的是新文件的锁（它每轮重新 open 当前路径，不缓存旧 fd）
    h.wait_job(j2, "running", timeout=10)
    holder2 = h.job(j2)
    must(holder2["locked_ino"] == new_ino, "j2 必须锁替换后的新文件")
    print(f"[5] {j2} 持新锁 inode={holder2['locked_ino']}（未继承旧 fd）")

    # 不杀任务，等它们自然结束，锁全部释放
    h.wait_job(j1, "done", timeout=20)
    h.wait_job(j2, "done", timeout=20)
    wait_for(lambda: h.snap() if not h.snap()["proc_locks"] else None,
             "所有 FLOCK 释放", timeout=15)
    print("[6] 两个备份自然结束，/proc/locks 已无 FLOCK 记录")
    print("场景 A：PASS")


def test_maintenance(h: Harness) -> None:
    print("\n=== 场景 B：检修停派、等待任务不抢跑、检修中再次换锁与重新核验 ===")
    ino_v1 = h.snap()["lock"]["inode"]

    # 先只派一个长任务并确认它持锁，避免检修前出现第二个 worker 的时序竞态。
    # 时长取 8s：足够覆盖检修开始与第一次失败核验，又能尽早自然结束。
    j1 = h.post("/api/backup", duration=8)["job_id"]
    h.wait_job(j1, "running", timeout=10)
    print(f"[1] {j1} 持锁 inode={ino_v1}")

    # 持锁期间立刻进入检修：调度器从此不得再 spawn 新 worker
    must(h.post("/api/maintenance/start", reason="冒烟测试检修")["ok"],
         "开始检修应成功")
    print("[2] 已进入检修，调度器停止 spawn")

    # 检修后发起三个任务：只能停在队列里，worker 进程不应被创建。
    # j2/j3 时长拉长，保证恢复后有足够窗口观察 waiting 与闸门拦截。
    # j2 时长要覆盖整个检修窗口（约 15s），j3/j4 更长以便放行后仍可观测
    j2 = h.post("/api/backup", duration=15)["job_id"]
    j3 = h.post("/api/backup", duration=20)["job_id"]
    j4 = h.post("/api/backup", duration=20)["job_id"]
    time.sleep(3)
    for jid in (j2, j3, j4):
        must(h.job(jid)["state"] == "queued", f"检修期间 {jid} 必须停留 queued")
        must(h.job(jid)["pid"] is None, f"{jid} 不应有 worker 进程")
    must(set(h.snap()["scheduler"]["queued"]) >= {j2, j3, j4},
         "j2/j3/j4 应留在调度队列中")
    print(f"[3] 检修后发起的三个任务 3s 后仍为 queued 且无 PID（未派发）")

    # 持锁未释放时尝试放行：核验必须拒绝并保持停派
    res = h.post("/api/maintenance/resume")
    must(res["ok"] is False, "持锁未释放时放行应被拒绝")
    must(h.snap()["maintenance"]["active"] is True, "核验失败后必须保持停派")
    print(f"[4] 持锁任务未结束时放行被拒，保持停派：{res['reason']}")

    # 不杀进程，等 j1 自行结束
    h.wait_job(j1, "done", timeout=20)

    # j1 结束后核验通过，队列恢复派发：j2 立即持锁，j3 进入取锁等待
    res = h.post("/api/maintenance/resume")
    must(res["ok"], f"旧锁释放后核验应通过，实际：{res}")
    print(f"[5] j1 自行结束，核验通过恢复派发：{res['reason']}")
    # j2 是队列头，恢复后应在秒级拿到锁；给 30s 余量防宿主机抖动
    h.wait_job(j2, "running", timeout=30)
    # j3 在 resume 前已被 spawn 成等待进程（在途上限内），锁释放后它要么
    # 仍在 waiting（j2 先抢到），要么已经 running；两种都合法，只要别是 queued
    j3_state = h.job(j3)["state"]
    must(j3_state in ("waiting", "running"),
         f"恢复后 j3 应已派发并参与取锁，实际 {j3_state}")
    if j3_state == "waiting":
        h.wait_job(j3, "waiting", timeout=10)  # 确认它稳定处于等待

    # j3 正在取锁路上时再次停派：worker 闸门必须把它拦回 waiting/maintenance
    must(h.post("/api/maintenance/start", reason="第二次检修，验证 worker 闸门")["ok"],
         "再次开始检修应成功")
    deadline = time.monotonic() + 8
    blocked = False
    while time.monotonic() < deadline:
        st = h.job(j3)
        if st["state"] == "waiting" and st.get("wait_reason") == "maintenance":
            blocked = True
            break
        time.sleep(0.4)
    must(blocked, "取锁等待中的 worker 在检修后必须被闸门拦回 waiting/maintenance")
    time.sleep(2)
    j3_now = h.job(j3)
    must(j3_now["state"] == "waiting", "闸门期间 j3 不得变为 running")
    # 调度器可能已把队列里的 j4 也 spawn 成等待进程；无论哪种形态，
    # 它都绝不能进入 running，且只要是 waiting 就必须被闸门挡住
    j4_now = h.job(j4)
    must(j4_now["state"] in ("queued", "waiting"),
         f"闸门期间 j4 不得 running，实际 {j4_now['state']}")
    if j4_now["state"] == "waiting":
        must(j4_now["wait_reason"] == "maintenance",
             "j4 若已在等待，原因必须是 maintenance 闸门")
    else:
        must(j4_now["pid"] is None, "j4 若仍 queued 则不应有 PID")
    print(f"[6] {j3}/{j4} 均被闸门拦截（未抢跑），状态="
          f"{j3_now['state']}/{j4_now['state']}")

    # 检修期间连续两次换锁（v2 -> v3），放行前必须按最新文件重新核验
    ino_v2 = h.post("/api/lock/replace", note="检修中第一次换锁 v2")["inode"]
    ino_v3 = h.post("/api/lock/replace", note="检修中再次换锁 v3")["inode"]
    must(len({ino_v1, ino_v2, ino_v3}) == 3, "三次锁文件 inode 应互不相同")
    must(h.snap()["lock"]["inode"] == ino_v3, "当前文件应为最新一代 v3")
    print(f"[7] 检修期间两次换锁：v2={ino_v2} -> v3={ino_v3}（当前）")
    # 此时 j2 仍持旧锁（v1 的 inode），核验必须继续报“持锁任务未结束”
    res = h.post("/api/maintenance/resume")
    must(res["ok"] is False, "j2 仍持旧锁时核验应失败")
    must(h.snap()["maintenance"]["active"] is True, "失败后保持停派")
    print(f"[7a] 旧锁未全释放，核验拒绝：{res['reason']}")

    # 模拟外部又动了锁：当前路径与受管最新一代记录不一致，核验必须报错并停派
    data_dir = os.environ.get("BM_DATA_DIR")
    if data_dir:
        rogue = os.path.join(data_dir, "locks", "rogue.lock")
        with open(rogue, "w", encoding="utf-8") as fh:
            fh.write("external replacement\n")
        os.replace(rogue, os.path.join(data_dir, "locks", "backup.lock"))
        res = h.post("/api/maintenance/resume")
        must(res["ok"] is False, "锁文件被外部替换后核验应失败")
        must("不一致" in res["reason"], f"原因应提示代次不一致，实际：{res['reason']}")
        must(h.snap()["maintenance"]["active"] is True, "出错后仍须保持停派")
        print(f"[8] 外部再次换锁，核验拒绝并保持停派：{res['reason']}")
        ino_v4 = h.post("/api/lock/replace", note="恢复受管最新一代 v4")["inode"]
    else:
        ino_v4 = ino_v3

    # 等 j2 自行结束，旧锁（v1 inode）才真正全数释放
    h.wait_job(j2, "done", timeout=30)

    # 全部旧锁已释放，按最新文件试探取锁成功，才放行
    res = h.post("/api/maintenance/resume")
    must(res["ok"], f"按最新文件核验应通过，实际：{res}")
    must(h.snap()["maintenance"]["active"] is False, "放行后检修标志应清除")
    print(f"[9] 按最新文件(inode={ino_v4})核验通过，恢复派发：{res['reason']}")

    # 放行后任务串行取锁（j3 先、j4 后），按最长链路给超时
    h.wait_job(j3, "running", timeout=30)
    must(h.job(j3)["locked_ino"] == ino_v4, "j3 放行后必须锁最新一代文件")
    # j4 排在队尾，观测窗口内可能已跑完；running/done 都接受，
    # 但都必须是锁着最新文件
    deadline = time.monotonic() + 35
    j4_final = None
    while time.monotonic() < deadline:
        st = h.job(j4)
        if st["state"] in ("running", "done"):
            j4_final = st
            break
        time.sleep(0.3)
    must(j4_final is not None, f"j4 未在窗口内开跑，实际 {h.job(j4)}")
    must(j4_final["locked_ino"] == ino_v4,
         f"j4 必须锁最新一代文件，实际 locked_ino={j4_final.get('locked_ino')}")
    print(f"[10] 放行后 {j3}/{j4} 先后开跑，均锁最新 inode={ino_v4}")

    h.wait_job(j3, "done", timeout=30)
    h.wait_job(j4, "done", timeout=30)
    wait_for(lambda: h.snap() if not h.snap()["proc_locks"] else None,
             "收尾 FLOCK 清空", timeout=30)
    print("场景 B：PASS")


def main() -> int:
    if len(sys.argv) > 1:
        proc, data = None, ""
        h = Harness(sys.argv[1])
    else:
        proc, base, data = start_server()
        h = Harness(base)
    try:
        test_page_render(h)
        test_dual_locks(h)
        test_maintenance(h)
    except Fail as exc:
        print(f"\n❌ 测试失败：{exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"\n❌ 测试异常：{exc!r}", file=sys.stderr)
        return 2
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
            shutil.rmtree(data, ignore_errors=True)
    print("\n🎉 全部场景通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
