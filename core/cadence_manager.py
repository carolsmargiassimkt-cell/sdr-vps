from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict

from core.db_manager import get_db_manager
from core.lead_state import LeadStateStore
from utils.safe_json import safe_read_json, safe_write_json


DEFAULT_CADENCE = {
    "cad_1": 0,
    "cad_2": 3600,
    "cad_3": 86400,
    "cad_4": 172800,
    "cad_5": 259200,
    "cad_6": 432000,
}


class CadenceManager:
    def __init__(
        self,
        config_file: Path | str,
        lead_state: LeadStateStore,
        processed_messages_file: Path | str,
    ) -> None:
        self.config_file = Path(config_file)
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        self.processed_messages_file = Path(processed_messages_file)
        self.processed_messages_file.parent.mkdir(parents=True, exist_ok=True)
        self.lead_state = lead_state
        self._lock = Lock()
        self._db = get_db_manager()
        if not self.config_file.exists():
            safe_write_json(self.config_file, dict(DEFAULT_CADENCE))

    def can_send(self, phone: Any, cadence_step: Any) -> bool:
        normalized = self.lead_state.normalize_phone(phone)
        if not normalized:
            return False
        state = self.lead_state.get(normalized)
        if not state:
            return False
        if str(state.get("status", "active")).strip().lower() in LeadStateStore.CLOSED_STATUSES:
            return False
        step = self.normalize_step(cadence_step)
        if self.is_duplicate(normalized, step):
            return False
        last_message_at = str(state.get("last_message_at", "")).strip()
        if not last_message_at:
            return True
        try:
            last_dt = datetime.fromisoformat(last_message_at)
        except ValueError:
            return True
        delay = self.get_delay_seconds(step)
        elapsed = (datetime.utcnow() - last_dt).total_seconds()
        return elapsed >= delay

    def get_delay_seconds(self, cadence_step: Any) -> int:
        payload = self._load_config()
        return int(payload.get(self.step_label(cadence_step), 0) or 0)

    def is_duplicate(self, phone: Any, cadence_step: Any) -> bool:
        key = self._processed_key(phone, cadence_step)
        return self._db.dispatch_exists(key)

    def mark_sent(self, phone: Any, cadence_step: Any) -> None:
        normalized = self.lead_state.normalize_phone(phone)
        key = self._processed_key(phone, cadence_step)
        self._db.insert_dispatch_log(
            dispatch_key=key,
            lead_key=normalized,
            channel="cadence",
            cadence_step=self.step_label(cadence_step),
            created_at=datetime.utcnow().isoformat(),
            payload={"phone": normalized, "cadence_step": self.step_label(cadence_step)},
        )

    def step_label(self, cadence_step: Any) -> str:
        step = self.normalize_step(cadence_step)
        return f"cad_{step}"

    @staticmethod
    def normalize_step(cadence_step: Any) -> int:
        digits = re.sub(r"\D+", "", str(cadence_step or "1"))
        return max(1, min(6, int(digits or "1")))

    def _processed_key(self, phone: Any, cadence_step: Any) -> str:
        normalized = self.lead_state.normalize_phone(phone)
        step = self.normalize_step(cadence_step)
        return f"{normalized}_cad{step}"

    def _load_config(self) -> Dict[str, Any]:
        with self._lock:
            try:
                payload = safe_read_json(self.config_file)
                return payload if isinstance(payload, dict) else dict(DEFAULT_CADENCE)
            except Exception:
                return dict(DEFAULT_CADENCE)
