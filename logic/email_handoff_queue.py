from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from utils.safe_json import safe_read_json, safe_write_json


class EmailHandoffQueue:
    def __init__(self, queue_file: Path | str, logger=None) -> None:
        self.queue_file = Path(queue_file)
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        if not self.queue_file.exists():
            safe_write_json(self.queue_file, [])

    def enqueue(self, item: Dict[str, Any]) -> None:
        payload = safe_read_json(self.queue_file)
        queue = payload if isinstance(payload, list) else []
        queue.append(dict(item or {}))
        safe_write_json(self.queue_file, queue)

    def pending(self) -> List[Dict[str, Any]]:
        payload = safe_read_json(self.queue_file)
        return [dict(item) for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []

