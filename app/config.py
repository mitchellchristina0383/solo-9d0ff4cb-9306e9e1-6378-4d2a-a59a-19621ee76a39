"""运行期路径与默认配置。

所有易变状态都放在 DATA_DIR 下，容器中该目录挂卷，便于复现与检查。
"""
import os

DATA_DIR = os.environ.get("BM_DATA_DIR", "/data")

STATE_DIR = os.path.join(DATA_DIR, "tasks")          # 每个备份任务一个 JSON 状态文件
LOCK_DIR = os.path.join(DATA_DIR, "locks")           # 锁文件目录
LOCK_PATH = os.path.join(LOCK_DIR, "backup.lock")    # 当前生效的锁文件路径
MAINT_PATH = os.path.join(DATA_DIR, "maintenance.json")
HISTORY_PATH = os.path.join(DATA_DIR, "lock_history.json")
EVENTS_PATH = os.path.join(DATA_DIR, "events.log")

# 默认样例备份持锁时长（秒），网页上可按次覆盖
DEFAULT_DURATION = int(os.environ.get("BM_DEFAULT_DURATION", "10"))
# 定时备份间隔（秒）；<=0 表示关闭定时派发，仅保留手动发起
SCHEDULE_INTERVAL = int(os.environ.get("BM_SCHEDULE_INTERVAL", "0"))
# 最多同时在途（排队/持锁）任务数
MAX_INFLIGHT = int(os.environ.get("BM_MAX_INFLIGHT", "6"))
# 后台调度循环周期（秒）
TICK_INTERVAL = float(os.environ.get("BM_TICK_INTERVAL", "0.3"))
# 取锁等待时的重试间隔（秒）
LOCK_RETRY_INTERVAL = float(os.environ.get("BM_LOCK_RETRY_INTERVAL", "0.25"))

for _d in (DATA_DIR, STATE_DIR, LOCK_DIR):
    os.makedirs(_d, exist_ok=True)
