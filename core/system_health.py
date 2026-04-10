from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any


_LOCK = Lock()


def log_system_health(project_root: Path | str, event: str, **fields: Any) -> None:
    path = Path(project_root) / "logs" / "system_health.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    line_parts = [datetime.utcnow().isoformat(), str(event or "").strip()]
    for key, value in fields.items():
        clean_key = str(key or "").strip()
        if not clean_key:
            continue
        clean_value = " ".join(str(value or "").strip().split())
        line_parts.append(f"{clean_key}={clean_value}")
    with _LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(" | ".join(line_parts) + "\n")
