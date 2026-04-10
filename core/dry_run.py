from __future__ import annotations

import os
from pathlib import Path

from core.system_health import log_system_health


def is_dry_run() -> bool:
    return str(os.getenv("DRY_RUN", "0")).strip().lower() in {"1", "true", "yes", "y"}


def log_dry_run(project_root: Path | str, worker_name: str) -> None:
    log_system_health(project_root, "dry_run_message_simulated", worker_name=worker_name)
