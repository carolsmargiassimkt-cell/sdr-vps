from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Dict

from core.system_health import log_system_health
from utils.safe_json import safe_read_json, safe_write_json


class RuntimeState:
    DEFAULT = {
        "last_scheduler_runs": {},
        "worker_restart_count": {},
        "last_heartbeat": {},
        "queue_sizes": {},
        "crm_blocks_today": 0,
        "messages_sent_today": 0,
    }

    def __init__(self, project_root: Path | str | None = None) -> None:
        root = Path(project_root or ".")
        self.state_file = root / "data" / "runtime_state.json"
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._autosave_thread: threading.Thread | None = None
        self._start_autosave()

    def load_state(self) -> None:
        payload = safe_read_json(self.state_file) if self.state_file.exists() else {}
        if isinstance(payload, dict):
            self._state = {**self.DEFAULT, **payload}
        else:
            self._state = dict(self.DEFAULT)
        log_system_health(self.state_file.parent.parent, "runtime_state_loaded")
        self.save_state()

    def save_state(self) -> None:
        with self._lock:
            safe_write_json(self.state_file, self._state)
        log_system_health(self.state_file.parent.parent, "runtime_state_saved")

    def _start_autosave(self) -> None:
        if self._autosave_thread and self._autosave_thread.is_alive():
            return
        self._autosave_thread = threading.Thread(target=self._autosave_loop, name="runtime-state-autosave", daemon=True)
        self._autosave_thread.start()

    def _autosave_loop(self) -> None:
        while True:
            time.sleep(60)
            try:
                self.save_state()
            except Exception:
                continue

    def update_worker_restart(self, worker_name: str) -> None:
        with self._lock:
            counters = self._state.setdefault("worker_restart_count", {})
            counters[worker_name] = int(counters.get(worker_name, 0) or 0) + 1

    def update_scheduler_run(self, job_name: str, timestamp: str) -> None:
        with self._lock:
            self._state.setdefault("last_scheduler_runs", {})[job_name] = timestamp

    def update_heartbeat(self, worker_name: str, timestamp: float) -> None:
        with self._lock:
            self._state.setdefault("last_heartbeat", {})[worker_name] = timestamp

    def update_queue_size(self, queue_name: str, size: int) -> None:
        with self._lock:
            self._state.setdefault("queue_sizes", {})[queue_name] = size

    def increment_crm_blocks(self) -> None:
        with self._lock:
            self._state["crm_blocks_today"] = int(self._state.get("crm_blocks_today", 0) or 0) + 1

    def increment_messages_sent(self) -> None:
        with self._lock:
            self._state["messages_sent_today"] = int(self._state.get("messages_sent_today", 0) or 0) + 1

    @property
    def state(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._state)
