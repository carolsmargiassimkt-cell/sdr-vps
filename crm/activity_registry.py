from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from utils.safe_json import safe_read_json, safe_write_json


class ActivityRegistry:
    def __init__(self, timeline_file: Path, pipedrive_client, logger) -> None:
        self.timeline_file = timeline_file
        self.crm = pipedrive_client
        self.logger = logger
        self.timeline_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.timeline_file.exists():
            safe_write_json(self.timeline_file, {})

    def _load(self) -> Dict[str, List[Dict[str, Any]]]:
        try:
            raw = safe_read_json(self.timeline_file)
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def _save(self, data: Dict[str, List[Dict[str, Any]]]) -> None:
        safe_write_json(self.timeline_file, data)

    @staticmethod
    def _sanitize_descricao(text: str) -> str:
        safe = (text or "").strip()
        if not safe:
            return ""
        # Requisito: nao mencionar bot/automacao/script
        safe = re.sub(r"\bbot\b", "assistente", safe, flags=re.IGNORECASE)
        safe = re.sub(r"\bautoma[cç][aã]o\b", "fluxo", safe, flags=re.IGNORECASE)
        safe = re.sub(r"\bscript\b", "processo", safe, flags=re.IGNORECASE)
        return safe

    def registrar_atividade(
        self,
        lead_id: str,
        descricao: str,
        person_id: int = 0,
        deal_id: int = 0,
    ) -> Dict[str, Any]:
        safe_desc = self._sanitize_descricao(descricao)
        if not safe_desc:
            raise ValueError("descricao vazia")

        now = datetime.now()
        event = {
            "timestamp": now.isoformat(),
            "hora": now.strftime("%H:%M"),
            "descricao": safe_desc,
        }
        key = str(lead_id or "sem_id")
        timeline = self._load()
        timeline.setdefault(key, []).append(event)
        self._save(timeline)
        self.logger.info(f"[CRM] atividade registrada para lead {key}: {safe_desc}")

        try:
            self.crm.create_note(person_id=person_id, deal_id=deal_id, content=safe_desc)
        except Exception as exc:
            self.logger.error(f"Erro ao criar nota no Pipedrive: {exc}")
        return event

    def get_timeline(self, lead_id: str) -> List[Dict[str, Any]]:
        timeline = self._load()
        return timeline.get(str(lead_id or "sem_id"), [])
