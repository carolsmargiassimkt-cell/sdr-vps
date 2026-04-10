from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Iterable, List

from core.system_health import log_system_health
from core.runtime_state import RuntimeState
from services.supervisor import Supervisor
from utils.safe_json import safe_write_json


class RuntimeHealthMonitor:
    STATUS_TEMPLATE = {
        "timestamp": "",
        "workers_running": 0,
        "queues_active": 0,
        "messages_sent_today": 0,
        "restarts_today": 0,
        "crm_blocks_today": 0,
        "system_status": "healthy",
    }

    def __init__(
        self,
        project_root: Path | str,
        supervisor: Supervisor,
        queue_managers: Iterable[object],
        runtime_state: RuntimeState,
        interval_sec: int = 30,
    ) -> None:
        self.project_root = Path(project_root)
        self.supervisor = supervisor
        self.queue_managers = list(queue_managers)
        self.runtime_state = runtime_state
        self.interval_sec = int(interval_sec)
        self.status_file = self.project_root / "logs" / "runtime_status.json"
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(target=self._monitor_loop, name="runtime-health-monitor", daemon=True)
        self._thread.start()

    def _monitor_loop(self) -> None:
        while True:
            try:
                self._write_status()
            except Exception:
                pass
            time.sleep(self.interval_sec)

    def _write_status(self) -> None:
        state = self.runtime_state.state
        payload = dict(self.STATUS_TEMPLATE)
        payload["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        payload["workers_running"] = len(self.supervisor.get_registered_worker_names())
        payload["queues_active"] = sum(
            int(queue.get_queue_metrics().get("pending", 0) or 0) for queue in self.queue_managers if queue
        )
        payload["messages_sent_today"] = int(state.get("messages_sent_today", 0) or 0)
        payload["restarts_today"] = sum(int(v or 0) for v in state.get("worker_restart_count", {}).values())
        payload["crm_blocks_today"] = int(state.get("crm_blocks_today", 0) or 0)
        safe_write_json(self.status_file, payload)
        log_system_health(self.project_root, "runtime_health_update")
