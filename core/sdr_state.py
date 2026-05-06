from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any


PIPELINE_ID = 7

STAGE_ENTRADA = 51
STAGE_NUTRICAO = 62
STAGE_PRONTO_PROSPECCAO = 63
STAGE_TENTATIVA_CONTATO = 52

LABEL_LEAD_TRAFEGO = "193"
LABEL_RESPONDIDO = "196"
LABEL_WARM_WHATSAPP = "226"
LABEL_FORM_RESPONDIDO = "form_respondido"
LABEL_CLICK_WARM = "clicked_warm"
LABEL_MAILCHIMP_CLICK = "mailchimp_click"

STATE_FILE = Path(os.getenv("SDR_STATE_FILE", "/root/sdr-vps/data/sdr_state.json"))
EVENTS_FILE = Path(os.getenv("SDR_EVENTS_FILE", "/root/sdr-vps/logs/sdr_events.jsonl"))

SCORE_WEIGHTS = {
    "email_open": 1,
    "email_click": 12,
    "form": 20,
    "calendly": 20,
    "email_reply": 25,
    "whatsapp_reply": 25,
    "linkedin_connected": 5,
    "decision_maker": 5,
    "icp_role": 5,
    "generic_email": -10,
    "shared_email_blocked": -20,
    "shared_phone_blocked": -20,
    "bounced_email": -20,
    "opt_out": -100,
    "wrong_contact": -50,
    "invalid_phone": -40,
}


def utcnow() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def load_state() -> dict[str, Any]:
    try:
        if STATE_FILE.exists():
            payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        print("[SDR_STATE_READ_FAIL]", exc)
    return {}


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def deal_key(deal_id: Any) -> str:
    return str(int(deal_id or 0))


def get_deal_state(deal_id: Any) -> dict[str, Any]:
    state = load_state()
    return dict(state.get(deal_key(deal_id)) or {})


def update_deal_state(deal_id: Any, **changes: Any) -> dict[str, Any]:
    key = deal_key(deal_id)
    state = load_state()
    current = dict(state.get(key) or {})
    current.setdefault("conversation_state", "new")
    current.setdefault("cadence_email_active", False)
    current.setdefault("cadence_wa_active", False)
    current.setdefault("wa_blocked", False)
    current.setdefault("lead_score", 0)
    current.update({k: v for k, v in changes.items() if v is not None})
    current["updated_at"] = utcnow()
    state[key] = current
    save_state(state)
    log_event("STATE_TRANSITION", deal_id=deal_id, changes=changes)
    return current


def log_event(event: str, **payload: Any) -> bool:
    row = {"ts": utcnow(), "event": event, **payload}
    try:
        EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with EVENTS_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as exc:
        print("[SDR_EVENT_LOG_FAIL]", event, exc)
    print(f"[{event}]", json.dumps(payload, ensure_ascii=False, default=str))
    return True


def update_score(deal_id: Any, phone: str = "", score_event: str = "", delta: int | None = None) -> int:
    current = get_deal_state(deal_id)
    weight = SCORE_WEIGHTS.get(str(score_event or ""), 0) if delta is None else int(delta)
    score = int(current.get("lead_score") or 0) + weight
    update_deal_state(deal_id, phone=phone or current.get("phone"), lead_score=score, last_score_event=score_event)
    log_event("LEAD_SCORE_UPDATE", deal_id=deal_id, phone=phone, score_event=score_event, delta=weight, lead_score=score)
    return score


def mark_email_cadence(deal_id: Any, email: str = "", phone: str = "", active: bool = True, status: str = "", **metadata: Any) -> dict[str, Any]:
    return update_deal_state(
        deal_id,
        email=email,
        phone=phone,
        origin=metadata.get("origin"),
        cadence_email_active=bool(active),
        email_cadence_status=status or ("active" if active else "stopped"),
        last_sender="email_cadence",
    )


def mark_warm(deal_id: Any, phone: str = "", source: str = "", score_event: str = "") -> dict[str, Any]:
    if score_event:
        update_score(deal_id, phone=phone, score_event=score_event)
    return update_deal_state(
        deal_id,
        phone=phone,
        conversation_state="warm",
        warm_source=source or "unknown",
        cadence_email_active=False,
        cadence_wa_active=True,
        last_sender="warm_signal",
    )


def mark_inbound(deal_id: Any, phone: str = "", text: str = "", intent: str = "", origin: str = "") -> dict[str, Any]:
    stopped = str(intent or "").lower() in {"opt_out", "wrong_contact", "negative_stop", "automation_blocked"}
    return update_deal_state(
        deal_id,
        phone=phone,
        conversation_state="stopped" if stopped else "replied",
        cadence_email_active=False,
        cadence_wa_active=False,
        last_inbound_at=utcnow(),
        last_lead_reply=text,
        last_intent=intent or "inbound",
        origin=origin or None,
        stopped=stopped,
    )


def stop_automation(deal_id: Any, phone: str = "", reason: str = "") -> dict[str, Any]:
    return update_deal_state(
        deal_id,
        phone=phone,
        conversation_state="stopped",
        cadence_email_active=False,
        cadence_wa_active=False,
        stopped=True,
        stop_reason=reason or "manual_stop",
    )


def add_label(*args: Any, **kwargs: Any) -> bool:
    log_event("CRM_LABEL_UPDATE", args=args, **kwargs)
    return True


def create_activity(*args: Any, **kwargs: Any) -> bool:
    log_event("CRM_ACTIVITY_CREATE", args=args, **kwargs)
    return True


def create_note(*args: Any, **kwargs: Any) -> bool:
    log_event("CRM_NOTE_CREATE", args=args, **kwargs)
    return True
