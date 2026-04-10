from __future__ import annotations

import threading
import time
from typing import Dict


class WorkerHeartbeat:
    def __init__(self) -> None:
        self._heartbeats: Dict[str, float] = {}
        self._lock = threading.Lock()

    def register_worker(self, worker_name: str) -> None:
        with self._lock:
            self._heartbeats.setdefault(worker_name, time.time())

    def beat(self, worker_name: str) -> None:
        with self._lock:
            if worker_name in self._heartbeats:
                self._heartbeats[worker_name] = time.time()

    def get_last_seen(self, worker_name: str) -> float | None:
        with self._lock:
            return self._heartbeats.get(worker_name)
