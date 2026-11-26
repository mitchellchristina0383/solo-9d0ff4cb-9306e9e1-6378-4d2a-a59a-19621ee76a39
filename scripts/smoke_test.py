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
    # 同时导出给测试进程：Harness.job 可直接读状态文件，外部换锁分支也会启用
    os.environ["BM_DATA_DIR"] = data
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


def wait_resume_ok(h: Harness, timeout: float = 20.0) -> dict:
    """重试“核验并恢复”直到通过。

    worker 先把 state 写成 done，几毫秒后才在 finally 里释放 flock；
    第一次核验可能恰好撞上这个窗口而被拒，重试到内核锁真正释放即可。
    这不是放松核验——核验不通过时接口仍会如实返回 ok=False。
    """
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = h.post("/api/maintenance/resume")
        if last.get("ok"):
            return last
        time.sleep(0.4)
    raise Fail(f"核验放行在 {timeout}s 内一直未通过：{last}")


def wait_holder(h: Harness, ids: list[str], timeout: float = 15.0
                ) -> tuple[str, list[str]]:
    """flock(LOCK_NB 轮询) 不保证 FIFO：从一批任务里找出唯一持锁者与等待者。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        states = {jid: h.job(jid) for jid in ids}
        last = {jid: j.get("state") for jid, j in states.items()}
        if all(j.get("pid") for j in states.values()):
            running = [j for j in ids if states[j]["state"] == "running"]
            waiting = [j for j in ids if states[j]["state"] == "waiting"]
            if len(running) == 1 and len(waiting) == len(ids) - 1:
                return running[0], waiting
        time.sleep(0.2)
    raise Fail(f"未能确定唯一持锁者/等待者，最后状态={last}")


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

    # j1 结束后核验通过，队列恢复派发。done 状态文件可能略早于 flock
    # 内核释放，用 wait_resume_ok 跨过这个无害微窗口。
    res = wait_resume_ok(h, timeout=20)
    must(h.snap()["maintenance"]["active"] is False, "放行后检修标志应清除")
    print(f"[5] j1 自行结束，核验通过恢复派发：{res['reason']}")

    # j2/j3/j4 在同一个 tick 被派发；flock 非阻塞轮询不保证 FIFO，
    # 谁先抢到锁都合法。确定唯一持锁者 holder 与两个取锁等待者 waiters。
    ids = [j2, j3, j4]
    holder, waiters = wait_holder(h, ids, timeout=15)
    must(h.job(holder)["locked_ino"] == ino_v1,
         f"恢复后持锁者 {holder} 应锁初始 inode={ino_v1}，"
         f"实际 {h.job(holder).get('locked_ino')}")
    print(f"[5a] {holder} 抢到锁(inode={ino_v1})，等待者={waiters}（不要求 FIFO）")

    # 持锁者正在运行时再次停派：两个取锁路上的 worker 都必须被闸门拦回
    # waiting/maintenance，且除原持锁者外不得新增 running。
    must(h.post("/api/maintenance/start", reason="第二次检修，验证 worker 闸门")["ok"],
         "再次开始检修应成功")
    deadline = time.monotonic() + 8
    blocked = False
    while time.monotonic() < deadline:
        if all(h.job(w).get("state") == "waiting"
               and h.job(w).get("wait_reason") == "maintenance"
               for w in waiters):
            blocked = True
            break
        time.sleep(0.3)
    must(blocked, "所有取锁等待中的 worker 在检修后必须被闸门拦回 waiting/maintenance")
    time.sleep(1.5)
    running_now = [j for j in ids if h.job(j)["state"] == "running"]
    must(set(running_now) == {holder},
         f"闸门期间只能有原持锁者 {holder} running，实际 {running_now}")
    for w in waiters:
        wj = h.job(w)
        must(wj["state"] == "waiting" and wj.get("wait_reason") == "maintenance",
             f"{w} 必须稳定停在 waiting/maintenance，"
             f"实际 {wj['state']}/{wj.get('wait_reason')}")
    print(f"[6] 等待者 {waiters} 均被闸门拦截（未抢跑），"
          f"持锁者 {holder} 继续运行")

    # 检修期间连续两次换锁（v2 -> v3），放行前必须按最新文件重新核验
    ino_v2 = h.post("/api/lock/replace", note="检修中第一次换锁 v2")["inode"]
    ino_v3 = h.post("/api/lock/replace", note="检修中再次换锁 v3")["inode"]
    must(len({ino_v1, ino_v2, ino_v3}) == 3, "三次锁文件 inode 应互不相同")
    must(h.snap()["lock"]["inode"] == ino_v3, "当前文件应为最新一代 v3")
    print(f"[7] 检修期间两次换锁：v2={ino_v2} -> v3={ino_v3}（当前）")
    # 此时 holder 仍持旧锁（v1 的 inode），核验必须继续报“持锁任务未结束”
    res = h.post("/api/maintenance/resume")
    must(res["ok"] is False, "持锁者仍持旧锁时核验应失败")
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

    # 等持锁者自行结束，旧锁（v1 inode）才真正全数释放
    h.wait_job(holder, "done", timeout=45)

    # 全部旧锁已释放，按最新文件试探取锁成功，才放行（跨过 flock 释放微窗口）
    res = wait_resume_ok(h, timeout=20)
    must(h.snap()["maintenance"]["active"] is False, "放行后检修标志应清除")
    print(f"[9] 按最新文件(inode={ino_v4})核验通过，恢复派发：{res['reason']}")

    # 两个等待者放行后串行取锁；不规定先后，只要求都锁最新一代文件并 done
    deadline = time.monotonic() + 85
    finals: dict[str, dict] = {}
    while time.monotonic() < deadline and len(finals) < len(waiters):
        for w in waiters:
            if w in finals:
                continue
            wj = h.job(w)
            if wj["state"] == "done":
                finals[w] = wj
            elif wj["state"] == "error":
                raise Fail(f"{w} 不应 error：{wj.get('note')}")
        if len(finals) < len(waiters):
            time.sleep(0.4)
    missing = [w for w in waiters if w not in finals]
    must(not missing,
         f"等待任务 {missing} 未在窗口内 done，实际 "
         f"{[(w, h.job(w)['state']) for w in missing]}")
    for w, wj in finals.items():
        must(wj["locked_ino"] == ino_v4,
             f"{w} 放行后必须锁最新 inode={ino_v4}，实际 {wj.get('locked_ino')}")
    print(f"[10] 放行后等待者 {waiters} 先后跑完，均锁最新 inode={ino_v4}")

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
