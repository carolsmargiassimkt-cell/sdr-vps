from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from core.production_safety import _locked_file


DEFAULT_TIMEOUT = float(os.getenv("STATE_LOCK_TIMEOUT_SECONDS", "10") or 10)


def _path(value: str | Path) -> Path:
    return Path(value)


def _lock_path(path: Path) -> str:
    return str(path) + ".lock"


def locked_load_json(path: str | Path, default: Any, *, timeout: float | None = None) -> Any:
    target = _path(path)
    lock = _lock_path(target)
    started = time.time()
    try:
        print(f"[STATE_LOCK_ACQUIRE] path={target}")
        with _locked_file(lock, timeout_seconds=float(timeout or DEFAULT_TIMEOUT)):
            if not target.exists():
                return default
            try:
                payload = json.loads(target.read_text(encoding="utf-8-sig") or "null")
                return payload if payload is not None else default
            except Exception:
                return default
    except TimeoutError:
        print(f"[STATE_LOCK_TIMEOUT] path={target} elapsed={time.time() - started:.2f}")
        return default
    finally:
        print(f"[STATE_LOCK_RELEASE] path={target}")


def locked_save_json(path: str | Path, data: Any, *, timeout: float | None = None) -> None:
    target = _path(path)
    lock = _lock_path(target)
    started = time.time()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        print(f"[STATE_LOCK_ACQUIRE] path={target}")
        with _locked_file(lock, timeout_seconds=float(timeout or DEFAULT_TIMEOUT)):
            tmp = target.with_suffix(target.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)
            try:
                dir_fd = os.open(str(target.parent), os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                pass
    except TimeoutError:
        print(f"[STATE_LOCK_TIMEOUT] path={target} elapsed={time.time() - started:.2f}")
        raise
    finally:
        print(f"[STATE_LOCK_RELEASE] path={target}")


def locked_append_jsonl(path: str | Path, row: dict[str, Any], *, timeout: float | None = None) -> None:
    target = _path(path)
    lock = _lock_path(target)
    started = time.time()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        print(f"[STATE_LOCK_ACQUIRE] path={target}")
        with _locked_file(lock, timeout_seconds=float(timeout or DEFAULT_TIMEOUT)):
            with target.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
    except TimeoutError:
        print(f"[STATE_LOCK_TIMEOUT] path={target} elapsed={time.time() - started:.2f}")
        raise
    finally:
        print(f"[STATE_LOCK_RELEASE] path={target}")
