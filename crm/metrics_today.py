from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict

from utils.safe_json import safe_read_json, safe_write_json


DEFAULT_METRICS = {
    "leads_hoje": 0,
    "whatsapp_enviados": 0,
    "emails_enviados": 0,
    "linkedin_follows": 0,
    "reunioes_marcadas": 0,
    "respostas_recebidas": 0,
}


@dataclass
class MetricsToday:
    file_path: Path
    logger: object

    def __post_init__(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self._save_payload(self._fresh_payload())

    def _today_key(self) -> str:
        return datetime.now().date().isoformat()

    def _fresh_payload(self) -> Dict[str, object]:
        return {"date": self._today_key(), "metrics": dict(DEFAULT_METRICS)}

    def _load_payload(self) -> Dict[str, object]:
        try:
            payload = safe_read_json(self.file_path)
            if not isinstance(payload, dict):
                return self._fresh_payload()
            if payload.get("date") != self._today_key():
                return self._fresh_payload()
            metrics = payload.get("metrics") or {}
            normalized = dict(DEFAULT_METRICS)
            for key in normalized:
                normalized[key] = int(metrics.get(key, 0) or 0)
            return {"date": self._today_key(), "metrics": normalized}
        except Exception:
            return self._fresh_payload()

    def _save_payload(self, payload: Dict[str, object]) -> None:
        safe_write_json(self.file_path, payload)

    def get_metrics(self) -> Dict[str, int]:
        payload = self._load_payload()
        self._save_payload(payload)
        return dict(payload["metrics"])

    def increment(self, key: str, amount: int = 1) -> Dict[str, int]:
        payload = self._load_payload()
        metrics = payload["metrics"]
        if key not in metrics:
            metrics[key] = 0
        metrics[key] = int(metrics[key]) + int(amount)
        self._save_payload(payload)
        try:
            self.logger.info(f"Metric updated: {key}={metrics[key]}")
        except Exception:
            pass
        return dict(metrics)
