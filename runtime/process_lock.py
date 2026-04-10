from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from config.config_loader import get_runtime_dir


class ProcessAlreadyRunningError(RuntimeError):
    pass


class ProcessLock:
    def __init__(self, name: str) -> None:
        runtime_dir = get_runtime_dir()
        self.name = str(name or "process").strip().lower() or "process"
        self.lock_path = runtime_dir / f"{self.name}.lock"
        self._fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            return
        try:
            self._fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            if self._remove_stale_lock():
                print("[LOCK] lock antigo removido automaticamente")
                self._fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            else:
                raise ProcessAlreadyRunningError(f"{self.name} already running")
        os.write(self._fd, str(os.getpid()).encode("utf-8"))

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            os.close(self._fd)
        finally:
            self._fd = None
            try:
                if self.lock_path.exists():
                    self.lock_path.unlink()
            except OSError:
                pass

    def __enter__(self) -> "ProcessLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    def _remove_stale_lock(self) -> bool:
        pid = self._read_lock_pid()
        if pid and self._pid_is_running(pid):
            return False
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
            return True
        except OSError:
            return False

    def _read_lock_pid(self) -> int:
        try:
            raw = self.lock_path.read_text(encoding="utf-8").strip()
        except OSError:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _pid_is_running(pid: int) -> bool:
        if int(pid or 0) <= 0:
            return False
        try:
            os.kill(int(pid), 0)
        except Exception:
            return False
        return True


@contextmanager
def guarded_process(name: str) -> Iterator[ProcessLock]:
    lock = ProcessLock(name)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()
