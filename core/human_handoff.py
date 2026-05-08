from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.locked_json_state import locked_load_json, locked_save_json


BASE_DIR = Path(__file__).resolve().parents[1]
HANDOFF_FILE = BASE_DIR / "data" / "human_handoff_state.json"


def normalize_phone(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_human_handoff_state() -> dict[str, dict[str, Any]]:
    payload = locked_load_json(HANDOFF_FILE, {})
    return payload if isinstance(payload, dict) else {}


def save_human_handoff_state(state: dict[str, dict[str, Any]]) -> None:
    locked_save_json(HANDOFF_FILE, state if isinstance(state, dict) else {})


def is_human_handoff(phone: Any) -> bool:
    clean = normalize_phone(phone)
    if not clean:
        return False
    state = load_human_handoff_state()
    rec = state.get(clean) or state.get(clean[2:] if clean.startswith("55") else f"55{clean}") or {}
    return str(rec.get("status") or "").strip().lower() == "human_handoff"


def set_human_handoff(phone: Any, *, deal_id: int = 0, person_id: int = 0, org_id: int = 0, reason: str = "manual_whatsapp_reply") -> bool:
    clean = normalize_phone(phone)
    if not clean:
        return False
    state = load_human_handoff_state()
    previous = state.get(clean) if isinstance(state.get(clean), dict) else {}
    now = _now()
    state[clean] = {
        **previous,
        "phone": clean,
        "deal_id": int(deal_id or previous.get("deal_id") or 0),
        "person_id": int(person_id or previous.get("person_id") or 0),
        "org_id": int(org_id or previous.get("org_id") or 0),
        "status": "human_handoff",
        "reason": str(reason or previous.get("reason") or "manual_whatsapp_reply"),
        "created_at": previous.get("created_at") or now,
        "updated_at": now,
    }
    save_human_handoff_state(state)
    print(f"[HUMAN_HANDOFF_SET] phone={clean} deal_id={state[clean]['deal_id']} reason={state[clean]['reason']}")
    return True


def clear_human_handoff(phone: Any) -> bool:
    clean = normalize_phone(phone)
    if not clean:
        return False
    state = load_human_handoff_state()
    removed = False
    for key in {clean, clean[2:] if clean.startswith("55") else f"55{clean}"}:
        if key in state:
            state.pop(key, None)
            removed = True
    if removed:
        save_human_handoff_state(state)
        print(f"[HUMAN_HANDOFF_CLEAR] phone={clean}")
    return removed

