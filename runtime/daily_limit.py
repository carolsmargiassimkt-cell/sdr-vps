from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Dict


class DailyLimitStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()

    @staticmethod
    def _today_key() -> str:
        return datetime.now().astimezone().date().isoformat()

    def _read(self) -> Dict[str, Any]:
        try:
            raw = self.path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _write(self, payload: Dict[str, Any]) -> None:
        tmp = self.path.with_suffix(f"{self.path.suffix}.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def _normalize_channel(self, channel: str) -> str:
        return str(channel or "").strip().lower()

    def _get_channel_state(self, payload: Dict[str, Any], channel: str) -> Dict[str, Any]:
        today = self._today_key()
        channels = payload.setdefault("channels", {})
        state = channels.get(channel)
        if not isinstance(state, dict) or str(state.get("date") or "") != today:
            state = {"date": today, "count": 0, "updated_at": datetime.utcnow().isoformat()}
            channels[channel] = state
        return state

    def current_count(self, channel: str) -> int:
        channel_key = self._normalize_channel(channel)
        with self._lock:
            payload = self._read()
            state = self._get_channel_state(payload, channel_key)
            self._write(payload)
            return int(state.get("count", 0) or 0)

    def increment(self, channel: str) -> int:
        channel_key = self._normalize_channel(channel)
        with self._lock:
            payload = self._read()
            state = self._get_channel_state(payload, channel_key)
            state["count"] = int(state.get("count", 0) or 0) + 1
            state["updated_at"] = datetime.utcnow().isoformat()
            self._write(payload)
            return int(state["count"])
