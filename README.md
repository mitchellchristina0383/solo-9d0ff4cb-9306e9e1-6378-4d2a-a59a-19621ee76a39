# 定时备份锁文件检修台（Flask + 真实 flock）

## 背景与要复现的问题

多个定时备份任务共用同一份锁文件（如 `/var/lock/backup.lock`）。当运维把锁文件
**替换**（rename）成一份新文件后：

- 已经持锁的旧任务仍持有**旧 inode** 上的 flock（路径已不指向它，但 fd 上的锁仍有效）；
- 新任务按同一路径打开并锁住的是**新 inode** 的文件；
- 于是两份“同名锁”同时被持有，互斥保护失效。

本项目提供一个 Linux 检修页，用**真实的 `fcntl.flock(LOCK_EX)`** 复现该场景，并给出
标准修复流程：停派 → 让已持锁任务自行结束 → 按最新文件重新核验 → 恢复派发。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8080
```

数据（任务状态、锁文件、换锁历史、事件日志）持久化在命名卷 `backup_data`（容器内 `/data`）。

环境变量（`docker-compose.yml` 已给默认值）：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `BM_DEFAULT_DURATION` | 10 | 样例备份默认持锁秒数，网页每次发起可覆盖 |
| `BM_SCHEDULE_INTERVAL` | 0 | 定时备份间隔秒数，0=关闭定时、仅手动发起 |
| `BM_MAX_INFLIGHT` | 6 | 同时在途（排队/持锁）任务上限 |
| `BM_TICK_INTERVAL` | 0.3 | 后台调度循环周期秒数 |
| `BM_LOCK_RETRY_INTERVAL` | 0.25 | worker 取锁轮询间隔秒数 |
| `BM_COMPRESS_POLL_INTERVAL` | 0.2 | 压缩子进程 CPU 采样/账目落盘间隔秒数 |
| `PORT` | 8080 | HTTP 端口 |

## 页面能做什么

- 设定样例备份的**运行（持锁）时长**并发起备份；
- 发起表单可勾选 **压缩样例**：拿到锁后**顺序启动两个压缩子进程**
  （`python -m app.compress_child`，真实生成/读取/压缩本地数据），
  任务表新增 **“压缩 CPU 耗时（u+s，从 0 起）”** 列，合计
  **用户态 `ru_utime` + 内核态 `ru_stime`**，2 秒自动刷新时可看实时增长，
  并标注 `统计中 / ✓计全 / ⚠未计全` 与两个子进程的落账进度；
- 压缩负载三档：**快速退出**（子进程约 0.1–0.2s，远快于 2s 刷新，
  专门验证“两次刷新之间跑完仍补计”）、**标准**（每个约 2 CPU 秒，可看实时
  跳动）、**长负载**（供中断 / 重启演练）；
- 每个在途任务行有 **中断** 按钮（`POST /api/job/<id>/interrupt`，
  向 worker 进程组发 SIGTERM，连同压缩子进程一起收掉；body `{"force":true}`
  可发 SIGKILL 模拟意外硬中断）；
- **替换锁文件**：在锁目录生成新一代文件（带随机 generation 标记），
  经 `fsync` + `os.replace` 原子换名，任何时刻都可换，检修中也可以；
- **开始检修（停派）** 与 **修复·核验并恢复派发**；
- 查看每个任务的状态、PID、等待原因、**持有的锁 inode/代次**；
- 查看内核视角的 **`/proc/locks`**（FLOCK 记录，并标注归属任务、代次、是否当前文件）；
- 查看换锁历史与事件日志；页面每 2 秒自动刷新。

### 压缩 CPU 是怎么做到“不漏、不重、不回落、跨重启还在”的

每个备份任务是独立 `app.worker` 子进程，压缩账只由该 worker 写自己的
`/data/tasks/<job_id>.json`，天然按任务隔离；换锁后并行的两个样例各有各的
worker / 子进程 PID / 账目，互不累加。

- **从 0 开始**：入队时就写入 `compression.cpu_seconds = 0` 的账。
- **运行中可见**：worker 每 `BM_COMPRESS_POLL_INTERVAL`（默认 0.2s）读
  `/proc/<压缩子进程pid>/stat` 的 `utime+stime`（clock ticks 换算秒），
  原子落盘实时值。
- **刷新间隙退出也计入 / 每份只计一次**：子进程退出的瞬间，worker 用
  `os.wait4(pid, WNOHANG)` 回收并直接拿到内核 `rusage.ru_utime+ru_stime`
  终值；终值进入 `committed`，由 `children[].accounted/counted_once` 守门，
  同一笔 `rusage` 绝不重复累加。quick 档整任务约 0.5s、快于网页 2s 刷新，
  第一次刷新就已是 `done / 计全`，但两个子进程的用量分文不少。
- **交接只增不减**：模型是“已回收子进程的终值之和 `committed` ＋ 当前活动
  子进程的 `/proc` 采样”，活动采样只做**替换**而非累加；wait4 微秒级终值
  不小于最后一次 tick（向下取整）采样，交接用终值替换采样，外层再对发布值
  做“不小于上一次”的钳制，任何抖动下总数都不回落。
- **正常结束才计全**：两个子进程都 `wait4` 回收且 `waitstatus==0` 才置
  `compression.complete = fully_counted = true`（页面“✓ 计全”）。
  子进程非零退出会判任务 `error` 并标未计全，不会谎报计全。
- **意外中断保留最后用量并标未计全**：SIGTERM 时 worker 回收活动子进程、
  保留终值/最后采样，写 `state=error` + `complete=false / status=incomplete /
  count_label=未计全`；SIGKILL 来不及补账，则由服务收割或重启回收路径
  `finalize_compression()` **原样保留最后一次原子落盘的 `cpu_seconds`**
  并补标“未计全”，绝不清零。
- **容器重启可查**：账目就在命名卷 `/data/tasks/*.json`；重启后
  `_reconcile_on_startup()` 把 PID 已死的 `running/waiting` 任务判 `error`
  并对压缩账做同样的“保留数值 + 未计全”标记。`docker compose restart`
  （或 down/up，只要不删卷）后刷新页面即可看到历史数值与计全状态。

任务 JSON 中与压缩相关的主要字段（同时在任务对象顶层放了镜像字段，方便查询）：

```jsonc
"compression_cpu_seconds": 4.204,
"compression_complete": true,            // = fully_counted = accounting_complete
"compression_status": "complete",        // pending | running | complete | incomplete
"compression_count_label": "已计全",      // 或 “未计全”
"compression": {
  "profile": "standard", "children_total": 2, "children_accounted": 2,
  "cpu_seconds": 4.204, "user_seconds": 4.08, "system_seconds": 0.12,
  "committed_cpu_seconds": 4.204,
  "children": [ {"index":1,"pid":652,"accounted":true,"counted_once":true,
                 "source":"wait4+rusage","cpu_seconds":2.10, "...": "..."},
                {"index":2, "...": "..."} ]
}
```

`POST /api/backup` 在原有 `duration` 之外接受：
`compression`（兼容 `compression_sample`/`compress`/`sample`，或
`type/kind/mode/job_type` 含 `compression`）、`profile=quick|standard|long`
（兼容 `quick/fast=true`）。`POST /api/job/<job_id>/interrupt`
接受 `{"force": true}`。

## 实现要点

### 进程模型

- 单容器单进程：waitress 提供多线程 HTTP，另有一个后台调度线程；
- 每个备份任务是 `app.worker` **独立子进程**（`start_new_session=True`，
  自成进程组，便于“中断”时 `killpg` 连同压缩子进程一起终止），
  自己对锁路径做真实 `fcntl.flock(LOCK_EX|LOCK_NB)` 轮询排他锁；
- 压缩样例的 worker 在持锁后再顺序 `Popen` 两个 `app.compress_child`
  （`close_fds=True`，**不继承 flock fd**），并用 `/proc` + `wait4` 计量；
- 任务状态以 JSON 原子写（临时文件 + `os.replace`）落盘到 `/data/tasks/`，
  压缩本地数据落在 `/data/compression/<job_id>/`。

### 两道停派闸门（需求：等待取锁的任务也不能趁检修开跑）

1. **调度器闸门**：检修标志落盘在 `/data/maintenance.json`，激活后调度线程
   不再 spawn 任何新 worker，新发起的任务只留在内存队列（状态 `queued`、无 PID）。
2. **worker 闸门**：已在途、正在取锁轮询的 worker 每轮先读检修标志，激活时
   不开文件、不取锁，状态置 `waiting / wait_reason=maintenance`。
   取锁用 `LOCK_NB` 轮询而非阻塞式 `flock`，保证闸门能被及时感知；
   成功取锁的瞬间还会再做一次双重核验（检修标志 + 文件 inode），
   消除“核验与取锁之间”的竞态。

### 不缓存旧 fd（需求：换锁后新任务锁同名新文件）

worker 每一轮取锁都**重新 `open()` 当前锁路径**；若当轮没抢到锁就立刻关闭该 fd
再重试。因此锁文件被替换后，worker 下一轮自然打开的是新文件，绝不会长期占着
旧文件描述符。持锁后通过 `fstat` 记录实际 inode 并展示在页面上。

### 修复流程（点“修复”后）

**不杀任何任务**，只停派，让已持锁任务按设定时长自行结束。恢复前依次核验，
任一步失败都保持停派，原因写回 `maintenance.json` 并显示在页面横幅：

1. 当前锁路径存在，且 inode 与换锁历史中的**最新一代**一致
   （检修期间又被外部换过会在这里被拒，要求重新换锁/核验）；
2. 不存在任何存活的 `running` 持锁任务；
3. 解析 `/proc/locks`：与本服务 worker PID 或历史各代 inode 相关的 FLOCK
   必须全部消失（覆盖“状态已结束但 fd 泄漏仍占旧文件”的情况）；
4. 对**最新文件**做一次非阻塞试探取锁，取锁瞬间再核 inode，
   成功后立即释放——证明当前文件确实可取锁。

全部通过才清除检修标志；等待中的 worker 下一轮轮询自行开跑，且锁的是最新文件。
维护标志解析失败按 fail-closed 处理（视为检修中，禁止取锁）。

### 目录结构

```
app/
  config.py          # 路径与可调参数（全部来自环境变量，默认 /data）
  common.py          # 原子 JSON、维护标志、/proc/locks 解析
  compression.py     # 压缩 CPU 账：/proc 采样、wait4 rusage 提交、单调钳制
  compress_child.py  # 压缩子进程入口：真实生成/读写 + zlib 压缩本地数据
  worker.py          # 备份子进程：闸门检查 + 真实 flock + 压缩编排/计量
  app.py             # Flask、调度线程、换锁、中断、停派/恢复核验、快照 API
  templates/index.html  # 单页检修台（原生 JS，2s 自动刷新）
scripts/smoke_test.py        # 锁/检修端到端冒烟测试（自动起停服务）
scripts/compression_test.py  # 压缩计量/中断/重启持久化端到端测试
Dockerfile / docker-compose.yml / requirements.txt（含测试用 requests）
```

## 实测结果

本次新增功能在交付环境（Linux 6.8 / 16 vCPU，**沙箱内没有 docker / dockerd /
docker socket，也无 root 或 user namespace，无法真正 `docker compose up`**）
用与镜像同源的方式验证：同一个仓库、同一份 `app/` 代码，用 Python 3.11 venv
直接跑 `python -m app.app`（即镜像 `CMD`），数据目录指向独立临时/命名目录，
并用“停进程 + 同 `BM_DATA_DIR` 重启”等价模拟容器重启。Docker 侧只做了
compose YAML 解析与 Dockerfile/`.dockerignore` 核对（`COPY app` 会带上新模块，
压缩仅用标准库 `zlib`，slim 镜像含 `zlib1g`）。

自动化：

- `scripts/compression_test.py` **全部 6 组通过**（C1 页面入口、C2 quick
  单调精确、C3 实时增长、C4 换锁并行隔离、C5 SIGTERM/SIGKILL、C6 重启持久化）；
- `scripts/smoke_test.py` 加固后**连续两轮通过**（含场景 A/B 与外部换锁分支）；
- 另用 jsdom 加载真实服务返回的页面并执行其 JS，13 项渲染/勾选/发起/中断
  点击断言全部通过。

### 压缩 CPU 样例实测（本次需求，按验收动作）

用页面实际调用的 `/api/*` 接口、并**严格按网页 2s 节奏拉 `/api/snapshot`**
走了一遍“快速退出 → 换锁并行 → 中断 → 重启”，典型记录：

1. **子进程快速退出 / 刷新间隙补计**：quick 档整任务约 **0.52s** 结束
   （两个子进程各约 0.13–0.15 CPU 秒，主要是解释器启动），第一次 2s 刷新时
   已经是 `done / 已计全 / 2/2`，`cpu=0.309s (u=0.278+s=0.031)`，
   总数精确等于两个子进程 `wait4 rusage` 之和；用 10–15ms 高频读状态文件
   连采 52 个点，**单调回落 0 次**，入队首点为 `0.0`。
2. **运行中可见 + 交接不回落**：standard 单任务可观测到
   `0.190 → 0.970 → … → 4.204s` 的实时增长；子进程交接点
   `children_accounted` 从 `0/2 → 1/2 → 2/2`，CPU 总数在交接前后只增不减，
   终值 `4.204s = child1 + child2`（误差 < 2e-6）。
3. **换锁并行、各算各的**：A 先持旧 inode（如 8729412），换锁得新 inode
   8729932 后发起 B，B 立即持新锁与 A 并行；两次刷新里 A/B 数值各自增长
   （如 A=3.676、B=1.770），终值分别 `4.227s / 4.187s`，四个子进程 PID
   两两不相交（一次运行 `A=[652,655] B=[654,656]`），账目无串号。
4. **中断一笔**：long 样例运行到 `cpu=2.760s / 统计中` 时点“中断”
   （SIGTERM 进程组），下次刷新为 `error / 未计全 / 1/2`，
   `cpu=2.863s`（把被收掉的活动子进程 rusage 也补上了，故略增，绝不减少），
   worker 与活动子进程均已退出。另测 `kill -9 -<pgid>` 硬中断：
   服务收割后**原样保留**最后落盘值（如 `0.600s`）并标 `incomplete/未计全`。
5. **重启后可查**：把服务进程 `kill -9`、并把当时在跑的 long worker 进程组
   也 `kill -9`（等价容器被强停），用**同一个数据目录**重启服务，
   `/api/snapshot`（网页表格数据源）里：已完成任务仍 `complete=true` 且数值
   不变；SIGTERM/SIGKILL 中断的两笔仍 `incomplete/未计全` 且 CPU>0；
   强停时在跑的任务经启动回收变为 `error / 未计全`，最后用量
   （`1.17→1.38s` 的最后落盘值）保留不清零。另用 PID=999999 的确定性 stale
   记录验证同一回收路径（`2.345s` 原样保留并标未计全）。

复现：

```bash
pip install -r requirements.txt        # 含脚本用的 requests
python scripts/compression_test.py     # 自建 8092/8093 服务与临时数据目录
python scripts/smoke_test.py           # 锁/检修回归
# 或人工网页：docker compose up --build 后 http://localhost:8080
#   勾选“压缩样例”→ 选“快速退出”→ 发起；再选“标准”发起 A、替换锁、发起 B；
#   选“长负载”发起后点行内“中断”；最后 docker compose restart 并刷新页面。
```

### 锁/检修回归（原有能力）

测试环境：Linux 6.8，Python 3.11 venv 直接运行同一份代码。加固后的
`scripts/smoke_test.py` 连续两轮全部通过（每轮独立临时数据目录）。
> 说明：`flock(LOCK_NB)` 轮询的多个等待者**不保证 FIFO**。旧脚本假定恢复后
> 固定是 j2 先拿锁，在高负载沙箱里曾两次撞到非 FIFO 调度（一次基线代码也
> 出现过）。现已把场景 B 改为自动识别“唯一持锁者 + 两个等待者”，所有安全
> 断言（闸门、双锁、换锁代次、外部换锁拒绝、最新 inode）一条未减。

### 场景 A：两份锁同时被持有（要求实测项）

1. j1 发起并持有初始锁（如 inode=8412229）；
2. j2 在 j1 持锁期间发起，进入 `waiting / wait_reason=locked`；
3. 网页替换锁文件，新 inode=8412231，j1 不受影响仍持旧锁；
4. `/proc/locks` 中**同时出现两把 FLOCK**，页面标注 j1→旧文件（非当前）、
   j2→当前文件；
5. j2 每轮重新 open，最终拿到的是**新 inode** 的锁，未继承旧 fd；
6. 两任务自然结束后 `/proc/locks` 无残留 FLOCK。

### 场景 B：检修停派 + 检修中再次换锁（要求实测项）

1. 检修激活后发起的任务 3 秒后仍为 `queued` 且无 PID（调度器闸门生效）；
2. 持锁未释放时点修复，核验拒绝并保持停派，页面给出具体任务/PID/inode 原因；
3. 持锁任务自然结束后放行，队列恢复派发；取锁路上的 worker 被 worker 闸门
   拦回 `waiting / maintenance`，不会抢跑；
4. 检修期间连续两次换锁（v2→v3），随后在数据目录外部再换一次锁，
   核验报“与最新一代锁记录不一致”并**保持停派**；
5. 经正规换锁回到受管最新一代（v4），且所有旧锁释放后，对 v4 试探取锁成功，
   才恢复派发；j3/j4 放行后开跑，锁定 inode 均为最新的 v4。

### 其他实测

- 定时备份（`BM_SCHEDULE_INTERVAL=5`）自动入队/派发正常；检修期间定时任务
  同样被两道闸门挡住，恢复后自动继续；
- worker 收到 SIGTERM 时置 `error` 并释放 flock，`/proc/locks` 中旧 inode 立即消失；
- 首页包含全部操作入口与 `/proc/locks` 视图，HTTP 200。

## 已知限制 / 未完成项

1. **本沙箱未能真正执行 `docker compose up --build` / `docker compose restart`**：
   环境中没有 docker / compose / dockerd 二进制、没有 `/var/run/docker.sock`、
   无 root/sudo、user namespace 也被禁用，无法起守护进程。已做的等价验证：
   用镜像同款 `python -m app.app`（Python 3.11 venv）跑同一份 `app/` 代码、
   同一套 `/data/tasks` 原子 JSON，并以“kill 进程 + 同 `BM_DATA_DIR` 重启”
   验证了命名卷持久化语义；compose YAML 已解析校验，Dockerfile/`.dockerignore`
   已核对会包含新模块。**在有 Docker 的目标环境首次交付时，请按上面的人工
   网页脚本实跑一次 `docker compose up --build` → `docker compose restart`
   复核。** 本沙箱也没有可用浏览器，网页交互是用 jsdom 加载真实页面执行其
   JS 验证的（含点击），未做真人截图级确认。
2. 单实例设计：状态中的内存队列依赖单进程，未做多副本协调；
   `waitress` 单进程多线程 + 一个后台调度线程，请勿横向扩副本。
3. 页面无鉴权，定位为内网/本机检修工具；暴露到网络前需自行加反向代理认证。
4. 任务状态文件保留最近 100 条（快照接口截断，文件仍在卷中），
   无自动清理/归档任务；压缩本地数据 `/data/compression/<job_id>/`
   同样暂无 GC（standard/long 每任务约十几 MB 输入/产物），需要时可定期清。
5. 压缩 CPU 只统计两个**直接子进程**各自的 `utime+stime`（其内部不再派子进程，
   故不含 `cu_utime/cs_utime` 口径）；中断场景若子进程被 SIGKILL 且来不及
   `wait4` 取终值，总数是“最后一次 /proc tick 采样”，精度为一个 tick（通常 10ms），
   并始终标为未计全——这是设计上“保留最后已记录用量”的语义，而非精确终账。
6. 自动化验证以端到端脚本为主（`scripts/*_test.py`），没有建 mock 单测套件；
   锁场景的等待者顺序按 `flock` 非 FIFO 处理。
