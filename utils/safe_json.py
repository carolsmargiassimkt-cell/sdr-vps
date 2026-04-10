from __future__ import annotations

import atexit
import copy
import json
import os
import threading
import time
from contextlib import contextmanager
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Callable, Iterator


_REGISTRY_LOCK = threading.Lock()
_STORE_REGISTRY: dict[str, "CachedJsonStore"] = {}


@contextmanager
def _sidecar_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                break
            except OSError:
                time.sleep(0.01)

        try:
            yield
        finally:
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass


class CachedJsonStore:
    DEFAULT_DEBOUNCE_SEC = 10.0

    def __init__(self, path: str | Path, *, default_factory: Callable[[], Any]) -> None:
        self.path = Path(path)
        self.default_factory = default_factory
        self._lock = threading.RLock()
        self._cache: Any | None = None
        self._dirty = False
        self._file_signature: tuple[int, int] | None = None
        self._flush_timer: threading.Timer | None = None

    def read(self) -> Any:
        with self._lock:
            self._load_if_needed()
            return copy.deepcopy(self._cache)

    def replace(self, data: Any, *, flush: bool = False, debounce_sec: float | None = None) -> None:
        with self._lock:
            self._load_if_needed()
            new_data = copy.deepcopy(data)
            force_flush = flush or not self.path.exists()
            if self._cache == new_data:
                self._dirty = False
                self._cancel_timer_locked()
                return
            self._cache = new_data
            self._dirty = True
            delay = self.DEFAULT_DEBOUNCE_SEC if debounce_sec is None else float(debounce_sec)
            if force_flush or delay <= 0:
                self._flush_locked()
            else:
                self._schedule_flush_locked(delay)

    def mutate(self, mutator: Callable[[Any], Any], *, flush: bool = False, debounce_sec: float | None = None) -> Any:
        with self._lock:
            self._load_if_needed()
            before = copy.deepcopy(self._cache)
            result = mutator(self._cache)
            if self._cache == before:
                self._dirty = False
                self._cancel_timer_locked()
                return copy.deepcopy(result)
            self._dirty = True
            delay = self.DEFAULT_DEBOUNCE_SEC if debounce_sec is None else float(debounce_sec)
            if flush or delay <= 0:
                self._flush_locked()
            else:
                self._schedule_flush_locked(delay)
            return copy.deepcopy(result)

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _load_if_needed(self) -> None:
        signature = self._stat_signature()
        if self._cache is not None:
            if self._dirty:
                return
            if signature == self._file_signature:
                return

        if not self.path.exists():
            self._cache = self.default_factory()
            self._file_signature = None
            self._dirty = False
            return

        with _sidecar_lock(self.path.with_suffix(self.path.suffix + ".lock")):
            try:
                raw = self.path.read_text(encoding="utf-8-sig").strip()
            except OSError:
                self._cache = self.default_factory()
                self._file_signature = None
                self._dirty = False
                return

        if not raw:
            self._cache = self.default_factory()
            self._file_signature = self._stat_signature()
            self._dirty = False
            return

        try:
            self._cache = json.loads(raw)
        except JSONDecodeError:
            self._cache = self.default_factory()
        self._file_signature = self._stat_signature()
        self._dirty = False

    def _flush_locked(self) -> None:
        if not self._dirty:
            return
        self._cancel_timer_locked()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.parent / f"{self.path.name}.{os.getpid()}.tmp"
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        with _sidecar_lock(lock_path):
            try:
                with temp_path.open("w", encoding="utf-8") as file_obj:
                    json.dump(self._cache, file_obj, ensure_ascii=False, indent=2)
                    file_obj.flush()
                os.replace(temp_path, self.path)
            finally:
                try:
                    if temp_path.exists():
                        temp_path.unlink()
                except OSError:
                    pass
        self._file_signature = self._stat_signature()
        self._dirty = False

    def _schedule_flush_locked(self, debounce_sec: float) -> None:
        self._cancel_timer_locked()
        timer = threading.Timer(float(max(0.01, debounce_sec)), self.flush)
        timer.daemon = True
        self._flush_timer = timer
        timer.start()

    def _cancel_timer_locked(self) -> None:
        if self._flush_timer is None:
            return
        self._flush_timer.cancel()
        self._flush_timer = None

    def _stat_signature(self) -> tuple[int, int] | None:
        try:
            stat = self.path.stat()
            return (int(stat.st_mtime_ns), int(stat.st_size))
        except OSError:
            return None


def get_json_store(path: str | Path, *, default_factory: Callable[[], Any] = dict) -> CachedJsonStore:
    file_path = str(Path(path).resolve())
    with _REGISTRY_LOCK:
        store = _STORE_REGISTRY.get(file_path)
        if store is None:
            store = CachedJsonStore(file_path, default_factory=default_factory)
            _STORE_REGISTRY[file_path] = store
        return store


def flush_all_json_stores() -> None:
    with _REGISTRY_LOCK:
        stores = list(_STORE_REGISTRY.values())
    for store in stores:
        try:
            store.flush()
        except Exception:
            continue


def safe_read_json(path: str | Path) -> Any:
    return get_json_store(path, default_factory=dict).read()


def safe_write_json(path: str | Path, data: Any) -> None:
    # Persistencia sincrona para reduzir janela de corrida entre processos.
    get_json_store(path, default_factory=dict).replace(data, flush=True)


atexit.register(flush_all_json_stores)
