from __future__ import annotations

import threading
import time
from typing import Dict

from core.system_health import log_system_health


from pathlib import Path
from typing import Optional


class CircuitBreaker:
    COOLDOWN_SEC = 300
    MAX_FAILURES = 5

    def __init__(self) -> None:
        self._state: Dict[str, Dict[str, object]] = {}
        self._lock = threading.Lock()
        self.project_root = Path(".")

    def configure(self, project_root: str | Path | None) -> None:
        if project_root:
            self.project_root = Path(project_root)

    def _ensure_worker(self, name: str) -> Dict[str, object]:
        if name not in self._state:
            self._state[name] = {"failures": 0, "state": "closed", "opened_at": 0.0}
        return self._state[name]

    def allow_execution(self, worker_name: str) -> bool:
        worker = self._ensure_worker(worker_name)
        state = str(worker["state"])
        now = time.time()
        if state == "closed":
            return True
        if state == "open":
            if now - float(worker["opened_at"] or 0.0) >= self.COOLDOWN_SEC:
                worker["state"] = "half_open"
                return True
            return False
        if state == "half_open":
            return True
        return True

    def record_success(self, worker_name: str) -> None:
        worker = self._ensure_worker(worker_name)
        with self._lock:
            worker["failures"] = 0
            worker["state"] = "closed"
            worker["opened_at"] = 0.0

    def record_failure(self, worker_name: str) -> None:
        worker = self._ensure_worker(worker_name)
        with self._lock:
            worker["failures"] = int(worker["failures"]) + 1
            state = str(worker["state"])
            if state == "half_open":
                self._open(worker_name, worker)
                return
            if state == "open":
                worker["opened_at"] = time.time()
                return
            if worker["failures"] >= self.MAX_FAILURES:
                self._open(worker_name, worker)

    def _open(self, worker_name: str, worker: Dict[str, object]) -> None:
        worker["state"] = "open"
        worker["opened_at"] = time.time()
        log_system_health(self.project_root, "circuit_breaker_triggered", worker_name=worker_name)

default_circuit_breaker = CircuitBreaker()
