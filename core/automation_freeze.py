from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parents[1]
FREEZE_FILE = BASE_DIR / "runtime" / "automation_freeze.json"
DEFAULT_TIMEZONE = "America/Sao_Paulo"


def _now(timezone_name: str = DEFAULT_TIMEZONE) -> datetime:
    return datetime.now(ZoneInfo(str(timezone_name or DEFAULT_TIMEZONE)))


def _parse_resume_at(value: Any, timezone_name: str = DEFAULT_TIMEZONE) -> datetime | None:
    token = str(value or "").strip()
    if not token:
        return None
    try:
        parsed = datetime.fromisoformat(token)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=ZoneInfo(str(timezone_name or DEFAULT_TIMEZONE)))
    return parsed


def load_automation_freeze(path: Path | None = None) -> Dict[str, Any]:
    target = Path(path or FREEZE_FILE)
    if not target.exists():
        return {
            "active": False,
            "resume_at": "",
            "timezone": DEFAULT_TIMEZONE,
            "services": [],
            "drop_inbound": False,
            "reason": "",
        }
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    services = payload.get("services")
    if not isinstance(services, list):
        services = []
    return {
        **payload,
        "active": bool(payload.get("active")),
        "resume_at": str(payload.get("resume_at") or "").strip(),
        "timezone": str(payload.get("timezone") or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE,
        "services": [str(item or "").strip().lower() for item in services if str(item or "").strip()],
        "drop_inbound": bool(payload.get("drop_inbound")),
        "reason": str(payload.get("reason") or "").strip(),
    }


def save_automation_freeze(payload: Dict[str, Any], path: Path | None = None) -> None:
    target = Path(path or FREEZE_FILE)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(dict(payload or {}), ensure_ascii=False, indent=2), encoding="utf-8")


def is_automation_freeze_active(*, service: str = "", now: datetime | None = None, path: Path | None = None) -> bool:
    payload = load_automation_freeze(path=path)
    if not bool(payload.get("active")):
        return False
    timezone_name = str(payload.get("timezone") or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    probe = now if isinstance(now, datetime) else _now(timezone_name)
    resume_at = _parse_resume_at(payload.get("resume_at"), timezone_name=timezone_name)
    if resume_at and probe >= resume_at:
        return False
    target_service = str(service or "").strip().lower()
    services = set(str(item or "").strip().lower() for item in list(payload.get("services") or []))
    if not target_service:
        return True
    if not services:
        return True
    return "all" in services or target_service in services


def should_drop_inbound(*, path: Path | None = None) -> bool:
    payload = load_automation_freeze(path=path)
    return bool(payload.get("drop_inbound")) and is_automation_freeze_active(service="inbound", path=path)


def freeze_resume_at(*, path: Path | None = None) -> str:
    return str(load_automation_freeze(path=path).get("resume_at") or "").strip()


def freeze_reason(*, path: Path | None = None) -> str:
    return str(load_automation_freeze(path=path).get("reason") or "").strip()
