from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from utils.safe_json import safe_read_json, safe_write_json


class LocalCRMCache:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def normalize_phone(value: Any) -> str:
        digits = re.sub(r"\D+", "", str(value or ""))
        if not digits:
            return ""
        if digits.startswith("55"):
            return digits
        if len(digits) in {10, 11, 12, 13}:
            return f"55{digits}"
        return digits

    @staticmethod
    def normalize_email(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def normalize_cnpj(value: Any) -> str:
        digits = re.sub(r"\D+", "", str(value or ""))
        return digits if len(digits) == 14 else ""

    @staticmethod
    def _coerce_list(values: Any) -> List[str]:
        if isinstance(values, list):
            items = values
        elif values in (None, "", []):
            items = []
        else:
            items = [values]
        output: List[str] = []
        seen = set()
        for item in items:
            clean = str(item or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            output.append(clean)
        return output

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def load(self) -> Dict[str, Any]:
        payload = safe_read_json(self.path) if self.path.exists() else {}
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("meta", {})
        payload.setdefault("records", [])
        if not isinstance(payload["records"], list):
            payload["records"] = []
        return payload

    def save(self, payload: Dict[str, Any]) -> None:
        safe_write_json(self.path, payload)

    def records(self) -> List[Dict[str, Any]]:
        return [dict(item) for item in self.load().get("records", []) if isinstance(item, dict)]

    def find_by_phone(self, phone: Any) -> Dict[str, Any]:
        normalized = self.normalize_phone(phone)
        if not normalized:
            return {}
        for record in self.records():
            phones = [self.normalize_phone(item) for item in self._coerce_list(record.get("phones"))]
            primary = self.normalize_phone(record.get("telefone") or record.get("phone"))
            if normalized == primary or normalized in phones:
                return self._flatten_record(record, normalized_phone=normalized)
        return {}

    def iter_targets(self, *, only_super_minas: bool = False, limit: int = 100) -> List[Dict[str, Any]]:
        output: List[Dict[str, Any]] = []
        seen = set()
        for record in self.records():
            if only_super_minas and not bool(record.get("super_minas")):
                continue
            lead = self._flatten_record(record)
            phone = self.normalize_phone(lead.get("telefone") or lead.get("phone"))
            email = self.normalize_email(lead.get("email"))
            dedupe_key = phone or email or str(lead.get("lead_id") or "").strip()
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            output.append(lead)
            if len(output) >= max(1, int(limit)):
                break
        return output

    def update_after_send(self, lead: Dict[str, Any], *, channel: str, cadence_step: int, tags: Iterable[str]) -> None:
        normalized_phone = self.normalize_phone(lead.get("telefone") or lead.get("phone"))
        if not normalized_phone:
            return

        payload = self.load()
        changed = False
        for record in payload.get("records", []):
            if not isinstance(record, dict):
                continue
            phones = [self.normalize_phone(item) for item in self._coerce_list(record.get("phones"))]
            primary = self.normalize_phone(record.get("telefone") or record.get("phone"))
            if normalized_phone not in phones and normalized_phone != primary:
                continue
            existing_tags = self._coerce_list(record.get("tags"))
            for tag in tags:
                clean_tag = str(tag or "").strip()
                if clean_tag and clean_tag not in existing_tags:
                    existing_tags.append(clean_tag)
            record["tags"] = existing_tags
            record["last_sent_channel"] = str(channel or "").strip().lower()
            record["last_sent_step"] = int(cadence_step or 0)
            record["last_sent_at"] = self._now_iso()
            record["updated_at"] = self._now_iso()
            changed = True
        if changed:
            payload["meta"]["updated_at"] = self._now_iso()
            self.save(payload)

    def update_status(self, lead: Dict[str, Any], *, status_bot: str = "", blacklist_reason: str = "") -> None:
        normalized_phone = self.normalize_phone(lead.get("telefone") or lead.get("phone"))
        if not normalized_phone:
            return
        payload = self.load()
        changed = False
        for record in payload.get("records", []):
            if not isinstance(record, dict):
                continue
            phones = [self.normalize_phone(item) for item in self._coerce_list(record.get("phones"))]
            primary = self.normalize_phone(record.get("telefone") or record.get("phone"))
            if normalized_phone not in phones and normalized_phone != primary:
                continue
            if status_bot:
                record["status_bot"] = str(status_bot).strip().lower()
            if blacklist_reason:
                record["blacklisted"] = True
                record["blacklist_reason"] = str(blacklist_reason).strip().lower()
            record["updated_at"] = self._now_iso()
            changed = True
        if changed:
            payload["meta"]["updated_at"] = self._now_iso()
            self.save(payload)

    @classmethod
    def _flatten_record(cls, record: Dict[str, Any], *, normalized_phone: str = "") -> Dict[str, Any]:
        phones = [cls.normalize_phone(item) for item in cls._coerce_list(record.get("phones"))]
        phones = [item for item in phones if item]
        emails = [cls.normalize_email(item) for item in cls._coerce_list(record.get("emails"))]
        emails = [item for item in emails if item]
        phone = normalized_phone or cls.normalize_phone(record.get("telefone") or record.get("phone")) or (phones[0] if phones else "")
        email = cls.normalize_email(record.get("email")) or (emails[0] if emails else "")
        tags = cls._coerce_list(record.get("tags"))
        return {
            "lead_id": str(record.get("lead_id") or f"local-{phone or email or record.get('cnpj') or record.get('empresa') or 'lead'}").strip(),
            "deal_id": int(record.get("deal_id", 0) or 0),
            "person_id": int(record.get("person_id", 0) or 0),
            "nome": str(record.get("nome") or record.get("contact_name") or record.get("empresa") or "").strip(),
            "empresa": str(record.get("empresa") or record.get("razao_social") or "").strip(),
            "telefone": phone,
            "email": email,
            "source": str(record.get("source") or "local_base").strip(),
            "tags": tags,
            "status_bot": str(record.get("status_bot") or "").strip().lower(),
            "stage_id": int(record.get("stage_id", 0) or 0),
            "sheet_day": str(record.get("sheet_day") or 1),
            "origem_oficial": str(record.get("origem_oficial") or "").strip(),
            "cnpj": cls.normalize_cnpj(record.get("cnpj")),
            "phones": phones,
            "emails": emails,
            "super_minas": bool(record.get("super_minas")),
            "blacklisted": bool(record.get("blacklisted")),
            "blacklist_reason": str(record.get("blacklist_reason") or "").strip(),
        }

