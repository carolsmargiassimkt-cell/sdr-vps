from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any, Dict, List

from utils.safe_json import safe_read_json, safe_write_json


class WhatsAppTargetCache:
    def __init__(self, cache_file: Path | str, *, ttl_sec: int = 300, logger=None) -> None:
        self.cache_file = Path(cache_file)
        self.ttl_sec = int(max(60, ttl_sec))
        self.logger = logger

    def install(self) -> None:
        from services.whatsapp_bot import WhatsAppBot

        if getattr(WhatsAppBot, "_target_cache_installed", False):
            return

        original = WhatsAppBot._fetch_targets_from_pipedrive
        cache = self

        def cached_fetch(bot: Any, limit: int = 100) -> List[Dict[str, Any]]:
            cached_items = cache._load_cached_items(bot=bot, limit=limit)
            if cached_items is not None:
                cache._log(f"[CACHE] whatsapp targets reutilizados | ttl={cache.ttl_sec}s | items={len(cached_items)}")
                return cached_items
            items = original(bot, limit=limit)
            if items:
                cache._save_cached_items(bot=bot, items=items)
            return items

        WhatsAppBot._fetch_targets_from_pipedrive = cached_fetch
        WhatsAppBot._target_cache_installed = True

    def _load_cached_items(self, *, bot: Any, limit: int) -> List[Dict[str, Any]] | None:
        if not self.cache_file.exists():
            return None
        payload = safe_read_json(self.cache_file)
        if not isinstance(payload, dict):
            return None
        meta = payload.get("meta") or {}
        items = payload.get("items") or []
        if not isinstance(meta, dict) or not isinstance(items, list):
            return None
        if meta.get("fingerprint") != self._fingerprint(bot):
            return None
        saved_at = float(meta.get("saved_at", 0) or 0)
        if saved_at <= 0 or (time.time() - saved_at) > self.ttl_sec:
            return None
        return [dict(item) for item in items[: int(limit)] if isinstance(item, dict)]

    def _save_cached_items(self, *, bot: Any, items: List[Dict[str, Any]]) -> None:
        payload = {
            "meta": {
                "saved_at": time.time(),
                "fingerprint": self._fingerprint(bot),
                "count": len(items),
            },
            "items": [dict(item) for item in items if isinstance(item, dict)],
        }
        safe_write_json(self.cache_file, payload)
        self._log(f"[CACHE] whatsapp targets atualizados | items={len(items)}")

    @staticmethod
    def _fingerprint(bot: Any) -> str:
        raw = "|".join(
            [
                str(getattr(getattr(bot, "config", None), "pipedrive_base_url", "") or "").strip(),
                str(getattr(getattr(bot, "config", None), "pipedrive_token", "") or "").strip()[:16],
                str(getattr(bot, "bot_name", "") or "").strip(),
            ]
        )
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _log(self, message: str) -> None:
        if self.logger and hasattr(self.logger, "info"):
            try:
                self.logger.info(message)
                return
            except Exception:
                pass
