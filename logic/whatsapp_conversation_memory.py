from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from utils.safe_json import safe_read_json, safe_write_json


class WhatsAppConversationMemory:
    def __init__(self, state_file: Path | str, logger=None) -> None:
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        if not self.state_file.exists():
            safe_write_json(self.state_file, {})

    @staticmethod
    def _normalize_phone(value: Any) -> str:
        digits = "".join(ch for ch in str(value or "") if ch.isdigit())
        return digits if digits.startswith("55") else f"55{digits}" if digits else ""

    def _read(self) -> Dict[str, Any]:
        payload = safe_read_json(self.state_file)
        return payload if isinstance(payload, dict) else {}

    def _write(self, payload: Dict[str, Any]) -> None:
        safe_write_json(self.state_file, payload)

    def find_by_phone(self, phone: Any) -> Dict[str, Any]:
        payload = self._read()
        return dict(payload.get(self._normalize_phone(phone)) or {})

    def register_manual_stop(self, phone: Any, *, reason: str) -> None:
        normalized = self._normalize_phone(phone)
        if not normalized:
            return
        payload = self._read()
        payload[normalized] = {
            **dict(payload.get(normalized) or {}),
            "status": "stop",
            "reason": str(reason or "").strip(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write(payload)

    def register_outbound(
        self,
        lead: Dict[str, Any],
        message: str,
        *,
        status: str,
        stage_number: int | None = None,
        intent: str = "",
        question_id: str = "",
    ) -> None:
        normalized = self._normalize_phone((lead or {}).get("telefone") or (lead or {}).get("phone"))
        if not normalized:
            return
        payload = self._read()
        current = dict(payload.get(normalized) or {})
        history = list(current.get("history") or [])
        history.append(
            {
                "direction": "outbound",
                "message": str(message or "").strip(),
                "status": str(status or "").strip(),
                "at": datetime.now(timezone.utc).isoformat(),
            }
        )
        payload[normalized] = {
            **current,
            "history": history[-50:],
            "last_message": str(message or "").strip(),
            "last_message_sent": str(message or "").strip(),
            "last_question_id": str(question_id or current.get("last_question_id") or "").strip(),
            "current_stage": int(stage_number or current.get("current_stage") or 1),
            "last_stage": int(stage_number or current.get("last_stage") or current.get("current_stage") or 1),
            "last_intent": str(intent or current.get("last_intent") or "").strip(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write(payload)

    def register_reply_context(
        self,
        lead: Dict[str, Any],
        inbound_messages: List[str],
        *,
        pitch_stage: str,
        suggested_reply: str,
        last_intent: str = "",
        current_stage: int | None = None,
    ) -> None:
        normalized = self._normalize_phone((lead or {}).get("telefone") or (lead or {}).get("phone"))
        if not normalized:
            return
        payload = self._read()
        current = dict(payload.get(normalized) or {})
        previous_stage = int(current.get("last_stage") or current.get("current_stage") or 0)
        target_stage = int(current_stage or previous_stage or 1)
        if previous_stage <= 0 and not current_stage:
            target_stage = 1
            if self.logger and hasattr(self.logger, "info"):
                self.logger.info("[STATE_INIT]")
        if target_stage < previous_stage:
            target_stage = previous_stage
            if self.logger and hasattr(self.logger, "info"):
                self.logger.info("[STATE_CORRIGIDO]")
        payload[normalized] = {
            **current,
            "last_inbound_messages": [str(item or "").strip() for item in inbound_messages if str(item or "").strip()][-10:],
            "pitch_stage": str(pitch_stage or "").strip(),
            "suggested_reply": str(suggested_reply or "").strip(),
            "last_question_id": str(current.get("last_question_id") or "").strip(),
            "last_intent": str(last_intent or current.get("last_intent") or "").strip(),
            "current_stage": target_stage,
            "last_stage": max(target_stage, previous_stage),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write(payload)

    def register_known_leads(self, leads: List[Dict[str, Any]]) -> None:
        payload = self._read()
        changed = False
        for lead in leads or []:
            normalized = self._normalize_phone((lead or {}).get("telefone") or (lead or {}).get("phone"))
            if not normalized:
                continue
            current = dict(payload.get(normalized) or {})
            payload[normalized] = {
                **current,
                "nome": str((lead or {}).get("nome") or "").strip(),
                "empresa": str((lead or {}).get("empresa") or "").strip(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            changed = True
        if changed:
            self._write(payload)
