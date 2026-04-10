from __future__ import annotations

import hashlib
import re
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterable, List

from core.db_manager import get_db_manager


class LeadStateStore:
    CLOSED_STATUSES = {"sem_interesse", "numero_errado", "encerrado", "stop", "failed"}
    SAVE_DEBOUNCE_SEC = 10.0

    def __init__(self, state_file: Path | str) -> None:
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._db = get_db_manager()
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._file_signature: tuple[int, int] | None = None
        self._phone_index: Dict[str, str] = {}
        self._email_index: Dict[str, str] = {}
        self._linkedin_index: Dict[str, str] = {}
        self._lead_id_index: Dict[str, str] = {}
        self._ensure_cache_loaded(force=True)

    @staticmethod
    def normalize_phone(raw: Any) -> str:
        digits = re.sub(r"\D+", "", str(raw or ""))
        if not digits:
            return ""
        return digits if digits.startswith("55") else f"55{digits}"

    @staticmethod
    def normalize_linkedin_profile(raw: Any) -> str:
        text = str(raw or "").strip().lower()
        if not text:
            return ""
        text = re.sub(r"\?.*$", "", text)
        return text.rstrip("/")

    @staticmethod
    def normalize_email(raw: Any) -> str:
        return str(raw or "").strip().lower()

    @classmethod
    def build_lead_id(cls, *, phone: Any = "", linkedin_profile: Any = "", email: Any = "") -> str:
        source = cls.normalize_phone(phone) or cls.normalize_linkedin_profile(linkedin_profile) or cls.normalize_email(email)
        if not source:
            return ""
        return hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]

    def all(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            self._ensure_cache_loaded()
            return {key: dict(value) for key, value in self._cache.items()}

    def get(self, phone: Any) -> Dict[str, Any]:
        with self._lock:
            self._ensure_cache_loaded()
            record = self._find_record_cached(phone=phone)
            return dict(record or {})

    def exists(self, phone: Any) -> bool:
        return bool(self.get(phone))

    def find_by_linkedin_profile(self, linkedin_profile: Any) -> Dict[str, Any]:
        with self._lock:
            self._ensure_cache_loaded()
            record = self._find_record_cached(linkedin_profile=linkedin_profile)
            return dict(record or {})

    def find_by_email(self, email: Any) -> Dict[str, Any]:
        with self._lock:
            self._ensure_cache_loaded()
            record = self._find_record_cached(email=email)
            return dict(record or {})

    def is_allowed(self, phone: Any) -> bool:
        state = self.get(phone)
        if not state:
            return False
        return not bool(state.get("blocked")) and str(state.get("status", "active")).strip().lower() not in {"blocked"}

    def upsert_lead(
        self,
        phone: Any,
        *,
        lead: Dict[str, Any] | None = None,
        bot: str = "",
        cadence_step: Any = "",
        status: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            self._ensure_cache_loaded()
            merged = self._upsert_lead_locked(
                phone,
                lead=dict(lead or {}),
                bot=bot,
                cadence_step=cadence_step,
                status=status,
            )
            return dict(merged)

    def upsert_from_crm_targets(self, leads: Iterable[Dict[str, Any]], *, bot: str = "") -> None:
        with self._lock:
            self._ensure_cache_loaded()
            changed = False
            for lead in leads or []:
                item = self._upsert_lead_locked(
                    lead.get("telefone") or lead.get("phone"),
                    lead=dict(lead or {}),
                    bot=bot,
                    cadence_step=lead.get("sheet_day") or lead.get("cadence_step") or 1,
                    status=str(lead.get("status") or ""),
                    defer_save=True,
                )
                if item:
                    changed = True
            if changed:
                self._persist_locked()

    def record_outbound(
        self,
        phone: Any,
        *,
        cadence_step: Any,
        bot: str = "",
        lead: Dict[str, Any] | None = None,
        status: str = "active",
    ) -> Dict[str, Any]:
        with self._lock:
            self._ensure_cache_loaded()
            current = self._upsert_lead_locked(phone, lead=dict(lead or {}), bot=bot, cadence_step=cadence_step, status=status, defer_save=True)
            if not current:
                return {}
            record_key = self._resolve_record_key_locked(
                phone=current.get("telefone"),
                email=current.get("email"),
                linkedin_profile=current.get("linkedin_profile"),
            )
            self._cache[record_key]["last_message_at"] = datetime.utcnow().isoformat()
            self._cache[record_key]["last_outbound_at"] = self._cache[record_key]["last_message_at"]
            self._cache[record_key]["updated_at"] = datetime.utcnow().isoformat()
            self._persist_locked()
            return dict(self._cache[record_key])

    def record_inbound(self, phone: Any, *, lead: Dict[str, Any] | None = None, status: str = "active") -> Dict[str, Any]:
        with self._lock:
            self._ensure_cache_loaded()
            current = self._upsert_lead_locked(phone, lead=dict(lead or {}), status=status, defer_save=True)
            if not current:
                return {}
            record_key = self._resolve_record_key_locked(
                phone=current.get("telefone"),
                email=current.get("email"),
                linkedin_profile=current.get("linkedin_profile"),
            )
            self._cache[record_key]["last_inbound_at"] = datetime.utcnow().isoformat()
            self._cache[record_key]["updated_at"] = datetime.utcnow().isoformat()
            self._persist_locked()
            return dict(self._cache[record_key])

    def mark_status(self, phone: Any, status: str, *, lead: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return self.upsert_lead(phone, lead=dict(lead or {}), status=status)

    def block_lead(self, *, phone: Any = "", email: Any = "", linkedin_profile: Any = "", reason: str = "lead_blocked") -> Dict[str, Any]:
        with self._lock:
            self._ensure_cache_loaded()
            current = self._find_record_cached(phone=phone, email=email, linkedin_profile=linkedin_profile)
            if not current:
                return self._upsert_lead_locked(
                    phone,
                    lead={"email": email, "linkedin_profile": linkedin_profile, "blocked": True, "tags": [reason]},
                    status="blocked",
                )
            current_tags = self._merge_tags(current.get("tags"), [reason])
            return self._upsert_lead_locked(
                current.get("telefone") or phone,
                lead={
                    **current,
                    "email": current.get("email") or email,
                    "linkedin_profile": current.get("linkedin_profile") or linkedin_profile,
                    "blocked": True,
                    "tags": current_tags,
                },
                status="blocked",
            )

    def close(self, phone: Any, status: str, *, lead: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return self.mark_status(phone, status, lead=lead)

    @staticmethod
    def build_dispatch_key(*, phone: Any = "", email: Any = "", channel: str = "", cadence_step: Any = "") -> str:
        channel_key = str(channel or "").strip().lower()
        step = str(cadence_step or "").strip()
        identity = LeadStateStore.normalize_phone(phone) or LeadStateStore.normalize_email(email)
        if not channel_key or not step or not identity:
            return ""
        return f"{identity}|{channel_key}|{step}"

    def reserve_dispatch(
        self,
        *,
        phone: Any = "",
        email: Any = "",
        channel: str,
        cadence_step: Any,
        payload: Dict[str, Any] | None = None,
    ) -> str:
        with self._lock:
            dispatch_key = self.build_dispatch_key(phone=phone, email=email, channel=channel, cadence_step=cadence_step)
            if not dispatch_key:
                return ""
            created_at = datetime.utcnow().isoformat()
            lead_key = self.build_lead_id(phone=phone, email=email)
            reserved = self._db.reserve_dispatch_log(
                dispatch_key=dispatch_key,
                lead_key=lead_key,
                channel=str(channel or "").strip().lower(),
                cadence_step=str(cadence_step or "").strip(),
                created_at=created_at,
                payload=dict(payload or {}),
            )
            return dispatch_key if reserved else ""

    def release_dispatch(self, dispatch_key: str) -> None:
        clean = str(dispatch_key or "").strip()
        if not clean:
            return
        with self._lock:
            self._db.delete_dispatch_log(clean)

    def latest_dispatch_at(self, channel: str) -> str:
        with self._lock:
            return self._db.get_latest_dispatch_created_at(str(channel or "").strip().lower())

    @staticmethod
    def _today_key() -> str:
        return datetime.now().astimezone().date().isoformat()

    def daily_send_count(self, channel: str) -> int:
        with self._lock:
            channel_key = str(channel or "").strip().lower()
            state_key = f"daily_send_counter:{channel_key}"
            payload = self._db.get_runtime_state(state_key)
            today = self._today_key()
            if str(payload.get("date") or "") != today:
                payload = {"date": today, "count": 0, "updated_at": datetime.utcnow().isoformat()}
                self._db.set_runtime_state(state_key, payload)
            return int(payload.get("count", 0) or 0)

    def increment_daily_send_count(self, channel: str) -> int:
        with self._lock:
            channel_key = str(channel or "").strip().lower()
            state_key = f"daily_send_counter:{channel_key}"
            payload = self._db.get_runtime_state(state_key)
            today = self._today_key()
            if str(payload.get("date") or "") != today:
                payload = {"date": today, "count": 0}
            payload["date"] = today
            payload["count"] = int(payload.get("count", 0) or 0) + 1
            payload["updated_at"] = datetime.utcnow().isoformat()
            self._db.set_runtime_state(state_key, payload)
            return int(payload["count"])

    @staticmethod
    def _clean_step(step: Any) -> int:
        digits = re.sub(r"\D+", "", str(step or "1"))
        return max(1, min(6, int(digits or "1")))

    @staticmethod
    def _merge_tags(*collections: Any) -> List[str]:
        tags: List[str] = []
        for collection in collections:
            if isinstance(collection, str):
                if collection.strip():
                    tags.append(collection.strip())
                continue
            if isinstance(collection, dict):
                continue
            if isinstance(collection, Iterable):
                for item in collection:
                    clean = str(item or "").strip()
                    if clean:
                        tags.append(clean)
        return sorted(set(tags))

    def _upsert_lead_locked(
        self,
        phone: Any,
        *,
        lead: Dict[str, Any],
        bot: str = "",
        cadence_step: Any = "",
        status: str = "",
        defer_save: bool = False,
    ) -> Dict[str, Any]:
        normalized_phone = self.normalize_phone(phone or lead.get("telefone") or lead.get("phone"))
        linkedin_profile = self.normalize_linkedin_profile(lead.get("linkedin_profile", ""))
        email = self.normalize_email(lead.get("email", ""))
        if not any((normalized_phone, linkedin_profile, email)):
            return {}
        record_key = self._resolve_record_key_locked(phone=normalized_phone, linkedin_profile=linkedin_profile, email=email)
        current = dict(self._cache.get(record_key, {}))
        tags = self._merge_tags(current.get("tags"), lead.get("tags"))
        blocked = bool(lead.get("blocked", current.get("blocked", False)))
        source_channel = str(lead.get("source_channel") or current.get("source_channel") or "").strip()
        name = str(lead.get("name") or lead.get("nome") or current.get("name") or current.get("nome") or "").strip()
        company = str(lead.get("empresa") or lead.get("company") or current.get("empresa") or "").strip()
        cadence_stage = str(
            lead.get("cadence_stage")
            or lead.get("cadencia_tag")
            or current.get("cadence_stage")
            or ""
        ).strip()
        merged = {
            "deal_id": int(lead.get("deal_id", current.get("deal_id", 0)) or 0),
            "person_id": int(lead.get("person_id", current.get("person_id", 0)) or 0),
            "organization_id": int(lead.get("organization_id", current.get("organization_id", 0)) or 0),
            "lead_id": str(
                lead.get("lead_id")
                or current.get("lead_id")
                or self.build_lead_id(phone=normalized_phone, linkedin_profile=linkedin_profile, email=email)
            ).strip(),
            "name": name,
            "nome": name,
            "empresa": company,
            "phone": normalized_phone or str(current.get("phone", current.get("telefone", ""))).strip(),
            "telefone": normalized_phone or str(current.get("telefone", current.get("phone", ""))).strip(),
            "email": email or str(current.get("email", "")).strip(),
            "linkedin_profile": linkedin_profile or str(current.get("linkedin_profile", "")).strip(),
            "source_channel": source_channel,
            "status": str(status or lead.get("status") or current.get("status") or "active").strip(),
            "cadence_step": int(self._clean_step(cadence_step or lead.get("sheet_day") or current.get("cadence_step") or 1)),
            "whatsapp_cadence_step": int(
                self._clean_step(
                    lead.get("whatsapp_cadence_step")
                    or current.get("whatsapp_cadence_step")
                    or cadence_step
                    or lead.get("sheet_day")
                    or current.get("cadence_step")
                    or 1
                )
            ),
            "email_cadence_step": int(
                self._clean_step(
                    lead.get("email_cadence_step")
                    or current.get("email_cadence_step")
                    or cadence_step
                    or lead.get("sheet_day")
                    or current.get("cadence_step")
                    or 1
                )
            ),
            "cadence_stage": cadence_stage,
            "cadence_tag": cadence_stage,
            "next_whatsapp_at": str(lead.get("next_whatsapp_at") or current.get("next_whatsapp_at") or "").strip(),
            "next_email_at": str(lead.get("next_email_at") or current.get("next_email_at") or "").strip(),
            "blocked": blocked,
            "bot": str(bot or current.get("bot") or "").strip(),
            "tags": tags,
            "last_message_at": str(current.get("last_message_at", "")).strip(),
            "last_inbound_at": str(current.get("last_inbound_at", "")).strip(),
            "last_outbound_at": str(current.get("last_outbound_at", "")).strip(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        self._cache[record_key] = merged
        self._index_record(record_key, merged)
        if not defer_save:
            self._persist_locked()
        return dict(merged)

    def _ensure_cache_loaded(self, *, force: bool = False) -> None:
        signature = self._stat_signature()
        if not force and self._cache and signature == self._file_signature:
            return
        payload = self._db.load_leads_map()
        self._cache = payload if isinstance(payload, dict) else {}
        self._file_signature = signature
        self._rebuild_indexes()

    def _persist_locked(self, *, flush: bool = False) -> None:
        self._db.replace_leads_map(self._cache)
        self._file_signature = self._stat_signature()

    def _rebuild_indexes(self) -> None:
        self._phone_index = {}
        self._email_index = {}
        self._linkedin_index = {}
        self._lead_id_index = {}
        for key, item in self._cache.items():
            if not isinstance(item, dict):
                continue
            self._index_record(key, item)

    def _index_record(self, key: str, item: Dict[str, Any]) -> None:
        phone = self.normalize_phone(item.get("telefone") or item.get("phone"))
        email = self.normalize_email(item.get("email", ""))
        linkedin = self.normalize_linkedin_profile(item.get("linkedin_profile", ""))
        lead_id = str(item.get("lead_id", "")).strip()
        if phone:
            self._phone_index[phone] = key
        if email:
            self._email_index[email] = key
        if linkedin:
            self._linkedin_index[linkedin] = key
        if lead_id:
            self._lead_id_index[lead_id] = key

    def _resolve_record_key_locked(
        self,
        *,
        phone: Any = "",
        linkedin_profile: Any = "",
        email: Any = "",
    ) -> str:
        existing = self._find_record_cached(phone=phone, linkedin_profile=linkedin_profile, email=email, include_key=True)
        if existing:
            return str(existing.get("__key__", "")).strip()
        normalized_phone = self.normalize_phone(phone)
        if normalized_phone:
            return normalized_phone
        return self.build_lead_id(phone=phone, linkedin_profile=linkedin_profile, email=email)

    def _find_record_cached(
        self,
        *,
        phone: Any = "",
        linkedin_profile: Any = "",
        email: Any = "",
        include_key: bool = False,
    ) -> Dict[str, Any]:
        normalized_phone = self.normalize_phone(phone)
        normalized_linkedin = self.normalize_linkedin_profile(linkedin_profile)
        normalized_email = self.normalize_email(email)
        key = ""
        if normalized_phone:
            key = self._phone_index.get(normalized_phone, "")
        if not key and normalized_linkedin:
            key = self._linkedin_index.get(normalized_linkedin, "")
        if not key and normalized_email:
            key = self._email_index.get(normalized_email, "")
        if not key:
            return {}
        result = dict(self._cache.get(key, {}))
        if include_key and result:
            result["__key__"] = key
        return result

    def _stat_signature(self) -> tuple[int, int] | None:
        try:
            stat = self._db.db_path.stat()
            return (int(stat.st_mtime_ns), int(stat.st_size))
        except OSError:
            return None


