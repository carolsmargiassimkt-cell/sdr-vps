from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List

from core.db_manager import get_db_manager
from utils.safe_json import safe_read_json, safe_write_json


class QueueManager:
    VALID_STATES = {"pending", "processing", "processed", "failed", "sent", "rejected"}
    REPLY_BUCKET_MINUTES = 10
    FAILED_COOLDOWN_SEC = 900
    MAX_RETRIES = 3
    SAVE_DEBOUNCE_SEC = 10.0

    def __init__(self, queue_file: Path | str, *, legacy_queue_file: Path | str | None = None) -> None:
        self.queue_file = Path(queue_file)
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        self.legacy_queue_file = Path(legacy_queue_file) if legacy_queue_file else None
        self._lock = RLock()
        self._db = get_db_manager()
        self._migrate_legacy_queue()

    def enqueue(
        self,
        *,
        phone: str,
        message: str,
        timestamp: str = "",
        crm_mapped: bool = False,
        status: str = "pending",
        source: str = "",
    ) -> Dict[str, Any]:
        clean_message = self._normalize_message(message)
        timestamp_value = timestamp or datetime.utcnow().isoformat()
        payload = self._load()
        item = {
            "id": self._build_id(phone, clean_message, self._timestamp_bucket(timestamp_value)),
            "phone": str(phone).strip(),
            "message": clean_message,
            "message_normalized": clean_message.lower(),
            "timestamp": timestamp_value,
            "timestamp_bucket": self._timestamp_bucket(timestamp_value),
            "crm_mapped": bool(crm_mapped),
            "status": status if status in self.VALID_STATES else "pending",
            "source": str(source or "").strip(),
            "attempts": 0,
            "retry_count": 0,
            "max_retries": self.MAX_RETRIES,
            "retry_after": "",
            "last_attempt_at": "",
            "last_error_at": "",
            "error": "",
            "channel": "reply_queue",
            "dedupe_key": self._reply_dedupe_key(phone, clean_message, timestamp_value),
        }
        duplicate = self._find_reply_duplicate(payload, item)
        if duplicate is not None:
            resolved = self._resolve_duplicate_enqueue(duplicate, item)
            self._save(payload)
            return dict(resolved)
        payload.append(item)
        self._save(payload)
        return dict(item)

    def enqueue_whatsapp_followup(
        self,
        *,
        lead_id: str,
        name: str,
        phone: str,
        source: str = "linkedin",
        status: str = "pending",
        message: str = "",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return self._enqueue_followup(
            channel="whatsapp",
            lead_id=lead_id,
            name=name,
            source=source,
            status=status,
            phone=phone,
            message=message,
            metadata=metadata,
        )

    def enqueue_email_followup(
        self,
        *,
        lead_id: str,
        name: str,
        email: str,
        source: str = "linkedin",
        status: str = "pending",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return self._enqueue_followup(
            channel="email",
            lead_id=lead_id,
            name=name,
            source=source,
            status=status,
            email=email,
            metadata=metadata,
        )

    def get_pending_whatsapp_leads(self, limit: int = 0) -> List[Dict[str, Any]]:
        return self._pending_followups("whatsapp", limit=limit)

    def get_pending_email_leads(self, limit: int = 0) -> List[Dict[str, Any]]:
        return self._pending_followups("email", limit=limit)

    def pending_items(self, limit: int = 0) -> List[Dict[str, Any]]:
        items = [
            item
            for item in self._load()
            if str(item.get("status", "")).strip() == "pending"
            and str(item.get("message", "")).strip()
            and self._cooldown_elapsed(item)
            and not self._is_stale_reply_noise(item)
        ]
        items.sort(key=lambda row: str(row.get("timestamp", row.get("created_at", ""))))
        if int(limit) > 0:
            return items[: int(limit)]
        return items

    def all_items(self) -> List[Dict[str, Any]]:
        items = self._load()
        items.sort(key=lambda row: str(row.get("timestamp", row.get("created_at", ""))))
        return items

    def mark_processing(self, item_id: str) -> Dict[str, Any]:
        return self._update(item_id, status="processing", attempts_increment=1)

    def mark_processed(self, item_id: str) -> Dict[str, Any]:
        return self._update(item_id, status="processed", error="", retry_after="")

    def mark_queue_processed(self, item_id: str) -> Dict[str, Any]:
        return self.mark_processed(item_id)

    def mark_failed(self, item_id: str, error: str = "") -> Dict[str, Any]:
        return self._update(item_id, status="failed", error=error, failed=True)

    def mark_rejected(self, item_id: str, error: str = "") -> Dict[str, Any]:
        return self._update(item_id, status="rejected", error=error, retry_after="")

    def requeue_failed(self, item_id: str) -> Dict[str, Any]:
        payload = self._load()
        for item in payload:
            if str(item.get("id", "")).strip() != str(item_id).strip():
                continue
            if not self._can_retry(item):
                return dict(item)
            if not self._cooldown_elapsed(item):
                return dict(item)
            item["status"] = "pending"
            item["error"] = ""
            item["retry_after"] = ""
            item["updated_at"] = datetime.utcnow().isoformat()
            self._save(payload)
            return dict(item)
        return {}

    def get_queue_metrics(self) -> Dict[str, int]:
        items = self._load()
        metrics = {"pending": 0, "processing": 0, "processed": 0, "failed": 0, "sent": 0, "rejected": 0}
        for item in items:
            status = str(item.get("status", "")).strip()
            if status in metrics:
                metrics[status] += 1
        return metrics

    def _enqueue_followup(
        self,
        *,
        channel: str,
        lead_id: str,
        name: str,
        source: str,
        status: str,
        phone: str = "",
        email: str = "",
        message: str = "",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = self._load()
        item = {
            "id": self._build_id(channel, lead_id or phone or email, source),
            "channel": channel,
            "lead_id": str(lead_id or "").strip(),
            "name": str(name or "").strip(),
            "phone": str(phone or "").strip(),
            "email": str(email or "").strip().lower(),
            "source": str(source or "").strip(),
            "status": status if status in self.VALID_STATES else "pending",
            "message": str(message or "").strip(),
            "metadata": dict(metadata or {}),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": "",
            "attempts": 0,
            "retry_count": 0,
            "max_retries": self.MAX_RETRIES,
            "retry_after": "",
            "last_attempt_at": "",
            "last_error_at": "",
            "error": "",
            "dedupe_key": self._followup_dedupe_key(channel, lead_id, phone, email, source),
        }
        duplicate = self._find_followup_duplicate(payload, item)
        if duplicate is not None:
            resolved = self._resolve_duplicate_enqueue(duplicate, item)
            self._save(payload)
            return dict(resolved)
        payload.append(item)
        self._save(payload)
        return dict(item)

    def _pending_followups(self, channel: str, *, limit: int = 0) -> List[Dict[str, Any]]:
        items = [
            item
            for item in self._load()
            if str(item.get("channel", "")).strip() == channel
            and str(item.get("status", "")).strip() == "pending"
            and self._cooldown_elapsed(item)
        ]
        items.sort(key=lambda row: str(row.get("created_at", "")))
        if int(limit) > 0:
            return items[: int(limit)]
        return items

    def _migrate_legacy_queue(self) -> None:
        if not self.legacy_queue_file or not self.legacy_queue_file.exists():
            return
        try:
            legacy_payload = safe_read_json(self.legacy_queue_file)
        except Exception:
            return
        if not isinstance(legacy_payload, list) or not legacy_payload:
            return
        current = self._load()
        changed = False
        for item in legacy_payload:
            if not isinstance(item, dict):
                continue
            messages = item.get("messages") or []
            message = ""
            if isinstance(messages, list) and messages:
                message = str(messages[-1]).strip()
            else:
                message = str(item.get("message", "")).strip()
            queue_item = {
                "id": self._build_id(item.get("phone", ""), message, self._timestamp_bucket(item.get("created_at", ""))),
                "phone": str(item.get("phone", "")).strip(),
                "message": self._normalize_message(message),
                "message_normalized": self._normalize_message(message).lower(),
                "timestamp": str(item.get("created_at", "")).strip() or datetime.utcnow().isoformat(),
                "timestamp_bucket": self._timestamp_bucket(item.get("created_at", "")),
                "crm_mapped": bool(item.get("crm_mapped")),
                "status": self._legacy_status(item),
                "source": str(item.get("source", "legacy_queue")).strip(),
                "attempts": int(item.get("attempts", 0) or 0),
                "retry_count": int(item.get("retry_count", 0) or 0),
                "max_retries": self.MAX_RETRIES,
                "retry_after": str(item.get("retry_after", "")).strip(),
                "last_attempt_at": str(item.get("last_attempt_at", "")).strip(),
                "last_error_at": str(item.get("last_error_at", "")).strip(),
                "error": str(item.get("error", "")).strip(),
                "channel": "reply_queue",
                "dedupe_key": self._reply_dedupe_key(item.get("phone", ""), message, item.get("created_at", "")),
            }
            if self._find_reply_duplicate(current, queue_item) is not None:
                continue
            current.append(queue_item)
            changed = True
        if changed:
            self._save(current, flush=True)

    @staticmethod
    def _parse_dt(raw: Any) -> datetime | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if getattr(parsed, "tzinfo", None) is not None:
                parsed = parsed.astimezone().replace(tzinfo=None)
            return parsed
        except Exception:
            return None

    def _is_stale_reply_noise(self, item: Dict[str, Any]) -> bool:
        if str(item.get("channel", "reply_queue")).strip() != "reply_queue":
            return False
        if bool(item.get("crm_mapped")):
            return False
        source = str(item.get("source", "")).strip().lower()
        if source not in {"legacy_queue", "manual_user_unread_list", "test"}:
            return False
        created_at = self._parse_dt(item.get("timestamp") or item.get("created_at") or item.get("updated_at"))
        if created_at is None:
            return False
        return created_at < (datetime.utcnow() - timedelta(days=1))

    @staticmethod
    def _legacy_status(item: Dict[str, Any]) -> str:
        if bool(item.get("processed")):
            return "processed" if bool(item.get("sent")) else "failed"
        return "pending"

    @staticmethod
    def _build_id(*parts: Any) -> str:
        raw = "|".join(str(part or "").strip() for part in parts)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_message(message: Any) -> str:
        return " ".join(str(message or "").strip().split())

    @classmethod
    def _reply_dedupe_key(cls, phone: Any, message: Any, timestamp: Any) -> str:
        normalized_phone = str(phone or "").strip()
        normalized_message = cls._normalize_message(message).lower()
        return cls._build_id("reply", normalized_phone, normalized_message, cls._timestamp_bucket(timestamp))

    @classmethod
    def _followup_dedupe_key(cls, channel: str, lead_id: Any, phone: Any, email: Any, source: Any) -> str:
        return cls._build_id(
            "followup",
            str(channel or "").strip(),
            str(lead_id or "").strip(),
            str(phone or "").strip(),
            str(email or "").strip().lower(),
            str(source or "").strip(),
        )

    @classmethod
    def _find_reply_duplicate(cls, items: List[Dict[str, Any]], candidate: Dict[str, Any]) -> Dict[str, Any] | None:
        candidate_key = str(candidate.get("dedupe_key", "")).strip()
        for item in items:
            if str(item.get("channel", "reply_queue")).strip() != "reply_queue":
                continue
            if str(item.get("dedupe_key", "")).strip() == candidate_key:
                return item
        return None

    @classmethod
    def _find_followup_duplicate(cls, items: List[Dict[str, Any]], candidate: Dict[str, Any]) -> Dict[str, Any] | None:
        candidate_key = str(candidate.get("dedupe_key", "")).strip()
        for item in items:
            if str(item.get("channel", "")).strip() != str(candidate.get("channel", "")).strip():
                continue
            if str(item.get("dedupe_key", "")).strip() == candidate_key:
                return item
        return None

    def _resolve_duplicate_enqueue(self, existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
        status = str(existing.get("status", "")).strip()
        if status in {"pending", "processing", "processed", "sent"}:
            return existing
        if status == "failed":
            if not self._can_retry(existing):
                return existing
            if not self._cooldown_elapsed(existing):
                return existing
            existing["status"] = "pending"
            existing["error"] = ""
            existing["retry_after"] = ""
            existing["updated_at"] = datetime.utcnow().isoformat()
            existing["timestamp"] = str(candidate.get("timestamp", existing.get("timestamp", ""))).strip()
            return existing
        return existing

    def _update(
        self,
        item_id: str,
        *,
        status: str,
        error: str | None = None,
        retry_after: str | None = None,
        attempts_increment: int = 0,
        failed: bool = False,
    ) -> Dict[str, Any]:
        payload = self._load()
        for item in payload:
            if str(item.get("id", "")).strip() != str(item_id).strip():
                continue
            item["status"] = status
            if error is not None:
                item["error"] = str(error or "").strip()
            if attempts_increment:
                item["attempts"] = int(item.get("attempts", 0) or 0) + int(attempts_increment)
                item["last_attempt_at"] = datetime.utcnow().isoformat()
            if failed:
                retry_count = max(
                    int(item.get("retry_count", 0) or 0) + 1,
                    int(item.get("attempts", 0) or 0),
                )
                item["retry_count"] = retry_count
                item["last_error_at"] = datetime.utcnow().isoformat()
                item["retry_after"] = retry_after or self._next_retry_after(retry_count)
            elif retry_after is not None:
                item["retry_after"] = str(retry_after or "").strip()
            item["updated_at"] = datetime.utcnow().isoformat()
            self._save(payload)
            return dict(item)
        return {}

    @classmethod
    def _can_retry(cls, item: Dict[str, Any]) -> bool:
        return int(item.get("retry_count", 0) or 0) < int(item.get("max_retries", cls.MAX_RETRIES) or cls.MAX_RETRIES)

    @staticmethod
    def _cooldown_elapsed(item: Dict[str, Any]) -> bool:
        retry_after = str(item.get("retry_after", "")).strip()
        if not retry_after:
            return True
        try:
            return datetime.utcnow() >= datetime.fromisoformat(retry_after)
        except ValueError:
            return True

    @classmethod
    def _next_retry_after(cls, retry_count: int) -> str:
        factor = max(1, min(int(retry_count or 1), cls.MAX_RETRIES))
        cooldown = cls.FAILED_COOLDOWN_SEC * factor
        return (datetime.utcnow() + timedelta(seconds=cooldown)).isoformat()

    @classmethod
    def _timestamp_bucket(cls, raw: Any) -> str:
        try:
            text = str(raw or "").strip()
            parsed = datetime.fromisoformat(text) if text else datetime.utcnow()
        except ValueError:
            parsed = datetime.utcnow()
        bucket_minutes = cls.REPLY_BUCKET_MINUTES
        floored_minute = (parsed.minute // bucket_minutes) * bucket_minutes
        return parsed.replace(minute=floored_minute, second=0, microsecond=0).isoformat()

    def _load(self) -> List[Dict[str, Any]]:
        with self._lock:
            payload = self._db.load_queue_items()
            return payload if isinstance(payload, list) else []

    def _save(self, payload: List[Dict[str, Any]], *, flush: bool = False) -> None:
        with self._lock:
            self._db.replace_queue_items(payload)
            safe_write_json(self.queue_file, payload)
