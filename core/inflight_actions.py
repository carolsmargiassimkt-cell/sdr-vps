from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.locked_json_state import locked_load_json, locked_save_json


BASE_DIR = Path(__file__).resolve().parents[1]
INFLIGHT_FILE = BASE_DIR / "data" / "inflight_actions.json"
TZ = ZoneInfo("America/Sao_Paulo")
TTL_MINUTES = 5


def now_brt() -> datetime:
    return datetime.now(TZ)


def _parse(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except Exception:
        return None


def _cleanup(data: dict[str, Any]) -> dict[str, Any]:
    current = now_brt()
    cleaned = {}
    for deal_id, item in dict(data or {}).items():
        expires = _parse((item or {}).get("expires_at") if isinstance(item, dict) else "")
        if expires and expires > current:
            cleaned[str(deal_id)] = item
    return cleaned


def acquire_inflight(deal_id: Any, channel: str) -> tuple[bool, str]:
    key = str(int(deal_id or 0))
    current_channel = str(channel or "").strip().lower()
    data = _cleanup(locked_load_json(INFLIGHT_FILE, {}))
    existing = data.get(key)
    if isinstance(existing, dict) and str(existing.get("channel") or "").lower() != current_channel:
        reason = f"other_channel_{existing.get('channel')}"
        print(f"[INFLIGHT_BLOCK] deal_id={key} channel={current_channel} reason={reason}")
        return False, reason
    now = now_brt()
    data[key] = {
        "channel": current_channel,
        "started_at": now.isoformat(timespec="seconds"),
        "expires_at": (now + timedelta(minutes=TTL_MINUTES)).isoformat(timespec="seconds"),
    }
    locked_save_json(INFLIGHT_FILE, data)
    print(f"[INFLIGHT_LOCK] deal_id={key} channel={current_channel}")
    return True, ""


def release_inflight(deal_id: Any, channel: str = "") -> None:
    key = str(int(deal_id or 0))
    data = _cleanup(locked_load_json(INFLIGHT_FILE, {}))
    existing = data.get(key)
    if isinstance(existing, dict):
        if channel and str(existing.get("channel") or "").lower() != str(channel or "").lower():
            return
        data.pop(key, None)
        locked_save_json(INFLIGHT_FILE, data)
    print(f"[INFLIGHT_RELEASE] deal_id={key} channel={str(channel or '').lower()}")
