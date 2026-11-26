"""原子写 JSON、维护状态读写、/proc/locks 解析等公共能力。"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any

from . import config


def atomic_write_json(path: str, payload: Any) -> None:
    """同目录临时文件 + os.replace，保证状态文件不会被读到半截内容。"""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def read_maintenance() -> dict:
    """读取检修状态。解析失败按 fail-closed 处理：视为检修中，禁止取锁。"""
    data = read_json(config.MAINT_PATH, None)
    if not isinstance(data, dict) or "active" not in data:
        return {"active": False}
    return data


def write_maintenance(payload: dict) -> None:
    atomic_write_json(config.MAINT_PATH, payload)


def file_identity(path: str) -> dict:
    """返回路径当前指向文件的身份信息；不存在时 exists=False。"""
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return {"exists": False, "inode": None, "device": None, "mtime": None}
    return {
        "exists": True,
        "inode": st.st_ino,
        "device": st.st_dev,
        "mtime": st.st_mtime,
    }


def parse_proc_locks() -> list[dict]:
    """解析 /proc/locks，只保留 POSIX fcntl.flock 产生的 FLOCK 记录。

    每行形如:
        3: FLOCK ADVISORY WRITE 1234 08:01:12345 0 EOF
    其中 08:01:12345 为 主设备:次设备:inode，EOF 表示整文件锁。
    容器内 PID 命名空间独立，pid 列即容器内可见的 PID。
    """
    rows: list[dict] = []
    try:
        with open("/proc/locks", "r", encoding="utf-8") as fh:
            lines = fh.readlines()
    except FileNotFoundError:
        return rows
    for line in lines:
        parts = line.split()
        if len(parts) < 7 or parts[1] != "FLOCK":
            continue
        ident = parts[5].split(":")
        if len(ident) != 3:
            continue
        try:
            major, minor, inode = int(ident[0]), int(ident[1]), int(ident[2])
            pid = int(parts[4])
        except ValueError:
            continue
        rows.append(
            {
                "kind": parts[1],
                "mode": parts[3],
                "pid": pid,
                "major": major,
                "minor": minor,
                "inode": inode,
            }
        )
    return rows
