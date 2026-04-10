from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional

from core.runtime_state import RuntimeState
from core.system_health import log_system_health
from services.email_leadster_worker import EmailLeadsterWorker
from services.metrics_reset_scheduler import MetricsResetScheduler
from utils.safe_json import safe_read_json, safe_write_json


class SystemSchedulerRunner:
    RUN_SLOTS = {
        "10:00": "leadster_email_ingestion",
        "16:00": "leadster_email_ingestion",
        "00:00": "metrics_aggregation",
    }
    MIN_LOOP_DELAY_SEC = 2
    MAX_LOOP_DELAY_SEC = 5

    def __init__(
        self,
        project_root: Path | str,
        *,
        logger: logging.Logger | None = None,
        runtime_state: RuntimeState | None = None,
    ) -> None:
        self.project_root = Path(project_root)
        self.logger = logger or logging.getLogger("system_scheduler_runner")
        self.state_file = self.project_root / "runtime" / "system_scheduler_state.json"
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.email_worker = EmailLeadsterWorker(self.project_root, logger=self.logger)
        self.metrics_scheduler = MetricsResetScheduler(self.project_root, logger=self.logger)
        self.extra_jobs: Dict[str, Callable[[], None]] = {}
        self.runtime_state = runtime_state

    def register_job(self, slot: str, callback: Callable[[], None]) -> None:
        clean_slot = str(slot or "").strip()
        if clean_slot:
            self.extra_jobs[clean_slot] = callback

    def run_forever(self) -> None:
        while True:
            executed = False
            try:
                executed = self.run_pending()
            except Exception as exc:
                self.logger.error(f"[SCHEDULER] falha no loop: {exc}")
            time.sleep(self._loop_delay_sec(executed=executed))

    def run_pending(self) -> bool:
        now = datetime.now()
        slot = now.strftime("%H:%M")
        if slot not in self.RUN_SLOTS:
            return False
        state = self._load_state()
        last_run = str(state.get("last_runs", {}).get(slot, "")).strip()
        if last_run.startswith(now.date().isoformat()):
            return False

        job_name = self.RUN_SLOTS[slot]
        if slot == "00:00":
            self.metrics_scheduler._reset_metrics()
        else:
            self.email_worker.run()
        extra_job = self.extra_jobs.get(slot)
        if callable(extra_job):
            extra_job()

        state.setdefault("last_runs", {})[slot] = now.isoformat()
        state["last_slot"] = slot
        state["updated_at"] = now.isoformat()
        safe_write_json(self.state_file, state)
        self.logger.info(f"[SCHEDULER] executed | slot={slot} | job={job_name}")
        log_system_health(self.project_root, "scheduler_run", slot=slot, job=job_name)
        if self.runtime_state:
            self.runtime_state.update_scheduler_run(job_name, state.setdefault("last_runs", {})[slot])
        return True

    def _load_state(self) -> Dict[str, object]:
        if not self.state_file.exists():
            return {}
        payload = safe_read_json(self.state_file)
        return payload if isinstance(payload, dict) else {}

    def _loop_delay_sec(self, *, executed: bool) -> int:
        if executed:
            return self.MIN_LOOP_DELAY_SEC
        return self.MAX_LOOP_DELAY_SEC
