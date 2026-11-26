"""压缩样例的子进程：真实压缩本地数据，产生用户态/内核态 CPU 开销。

每个样例任务在拿到 flock 后 *顺序* 启动本模块两次（--index 1/2）。
负载档位见 ``app.compression.PROFILES``：

* quick    —— 只生成并压缩一个小块后立即退出（用于验证子进程在网页两次
              刷新之间跑完时，wait4 rusage 仍会把用量计入原任务）；
* standard —— 反复压缩本地数据直到达到目标 CPU 秒数，便于页面观察实时增长；
* long     —— 长负载，供手动中断 / 容器重启演练。

所有数据都写在 ``$BM_DATA_DIR/compression/<job_id>/`` 下，可随时检查。
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import zlib

from . import config
from .compression import PROFILES, normalize_profile


def _cpu_seconds() -> float:
    t = os.times()
    return float(t.user + t.system)


def _build_block(size_mb: int) -> bytes:
    """造一块“有一定结构、可压缩但不极端”的数据。

    纯重复字节会让 deflate 过快、CPU 开销不足；这里用线性同余生成
    带变化的文本型内容，压缩过程会真实消耗用户态 CPU。
    """
    width = 1024 * 1024
    size = max(1, int(size_mb)) * width
    seed = 20261126
    out = bytearray(size)
    # 按 64 字节行写编号 + 伪文本，兼顾可压缩性与 deflate 计算量
    line = bytearray(64)
    pos = 0
    n = 0
    while pos < size:
        seed = (1103515245 * seed + 12345) & 0x7FFFFFFF
        token = (f"backup-block n={n:08d} r={seed:010d} "
                 .encode("ascii"))
        line[:] = b"compression-sample-" * 4
        line[0:len(token)] = token[:64]
        end = min(pos + 64, size)
        out[pos:end] = line[: end - pos]
        pos += 64
        n += 1
    return bytes(out)


def run(job_id: str, index: int, profile_name: str) -> int:
    profile_name = normalize_profile(profile_name)
    spec = PROFILES[profile_name]
    work_dir = os.path.join(config.DATA_DIR, "compression", job_id)
    os.makedirs(work_dir, exist_ok=True)
    input_path = os.path.join(work_dir, f"child{index}.input")
    output_path = os.path.join(work_dir, f"child{index}.gz")

    block_mb = int(spec["chunk_mb"])  # type: ignore[arg-type]
    block = _build_block(block_mb)
    # 落盘本地数据（产生内核态文件 IO/CPU），再读回压缩
    with open(input_path, "wb") as fh:
        fh.write(block)
        fh.flush()
        os.fsync(fh.fileno())

    target_cpu = float(spec["target_cpu"])  # type: ignore[arg-type]
    max_rounds = int(spec["max_rounds"])    # type: ignore[arg-type]
    max_wall = float(spec["max_wall"])      # type: ignore[arg-type]
    deadline = time.monotonic() + max_wall
    rounds = 0
    # quick 档 target_cpu=0：保证至少完整压缩一轮
    while True:
        payload = block
        try:
            with open(input_path, "rb") as fh:
                payload = fh.read()
        except OSError:
            pass
        compressed = zlib.compress(payload, 6)
        with open(output_path, "wb") as fh:
            fh.write(compressed)
            fh.flush()
            os.fsync(fh.fileno())
        rounds += 1
        if rounds >= max_rounds:
            break
        if target_cpu <= 0.0 and rounds >= 1:
            break
        if _cpu_seconds() >= target_cpu:
            break
        if time.monotonic() >= deadline:
            break
    # 输出一行自检信息到 worker 日志（不参与计量，计量由父进程 wait4 完成）
    print(f"[compress_child] job={job_id} idx={index} profile={profile_name} "
          f"rounds={rounds} cpu={_cpu_seconds():.3f}s "
          f"out={os.path.getsize(output_path)}B", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="压缩样例子进程")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--index", type=int, required=True, choices=(1, 2))
    parser.add_argument("--profile", default="standard")
    args = parser.parse_args()
    try:
        return run(args.job_id, args.index, args.profile)
    except Exception as exc:  # 子进程异常退出，父进程 wait4 会取到非 0 码
        print(f"[compress_child] idx={args.index} failed: {exc!r}",
              file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
