from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from utils.safe_json import safe_read_json, safe_write_json


class NotificationsCenter:
    def __init__(self, queue_file: Path, call_tasks_file: Path, logger) -> None:
        self.queue_file = queue_file
        self.call_tasks_file = call_tasks_file
        self.logger = logger
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        self.call_tasks_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.queue_file.exists():
            self._save(self.queue_file, [])
        if not self.call_tasks_file.exists():
            self._save(self.call_tasks_file, [])

    @staticmethod
    def _load(path: Path) -> List[Dict[str, Any]]:
        try:
            data = safe_read_json(path)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    @staticmethod
    def _save(path: Path, data: List[Dict[str, Any]]) -> None:
        safe_write_json(path, data)

    def enqueue_event(self, event_type: str, lead: Dict[str, Any], descricao: str) -> Dict[str, Any]:
        now = datetime.now()
        event = {
            "tipo": (event_type or "").strip(),
            "lead": lead or {},
            "data": now.isoformat(),
            "descricao": (descricao or "").strip(),
        }
        queue = self._load(self.queue_file)
        queue.append(event)
        self._save(self.queue_file, queue)
        self.logger.info(f"[QUEUE] evento criado: {event.get('tipo')}")

        if event["tipo"] == "lead_pediu_ligacao":
            self.enqueue_call_task(lead=lead, descricao=descricao)
        return event

    def enqueue_call_task(self, lead: Dict[str, Any], descricao: str) -> Dict[str, Any]:
        task = {
            "tipo": "ligacao",
            "lead": lead or {},
            "data": datetime.now().isoformat(),
            "descricao": (descricao or "").strip(),
            "status": "pendente",
        }
        tasks = self._load(self.call_tasks_file)
        tasks.append(task)
        self._save(self.call_tasks_file, tasks)
        self.logger.info("[QUEUE] tarefa de ligacao criada")
        return task

    def list_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        queue = self._load(self.queue_file)
        if limit <= 0:
            return queue
        return queue[-limit:]
