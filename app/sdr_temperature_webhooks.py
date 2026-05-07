from __future__ import annotations

import os
import re
from typing import Any

from fastapi import APIRouter, Request

from core.sdr_temperature import apply_temperature_to_deal, extract_calendly, normalize_phone


router = APIRouter()


def apply_enabled() -> bool:
    return str(os.getenv("SDR_TEMPERATURE_APPLY", "false") or "false").strip().lower() in {"1", "true", "yes", "on"}


def first_value(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def extract_leadster_signal(payload: dict[str, Any]) -> str:
    raw = " ".join(str(payload.get(key) or "") for key in ("event", "type", "event_type", "status", "source")).lower()
    if any(token in raw for token in ("form", "qualified", "qualificado")):
        return "leadster_form" if "qualified" not in raw and "qualificado" not in raw else "leadster_chat_qualified"
    return "leadster_visit"


@router.post("/webhooks/calendly")
async def calendly_webhook(req: Request) -> dict[str, Any]:
    payload = await req.json()
    event = str(payload.get("event") or payload.get("type") or "").strip()
    signal_type = "calendly_booked" if event == "invitee.created" else "calendly_canceled" if event == "invitee.canceled" else event
    calendly = extract_calendly(payload)
    enriched = {
        **payload,
        **calendly,
        "email": calendly.get("email") or first_value(payload, "email", "invitee_email"),
        "phone": calendly.get("phone") or normalize_phone(first_value(payload, "phone", "whatsapp", "text_reminder_number")),
        "source": "calendly",
    }
    result = apply_temperature_to_deal(
        int(payload.get("deal_id") or 0),
        signal_type=signal_type,
        payload=enriched,
        dry_run=not apply_enabled(),
    )
    return {"ok": True, "applied": apply_enabled(), "result": result}


@router.post("/webhooks/leadster")
async def leadster_webhook(req: Request) -> dict[str, Any]:
    payload = await req.json()
    signal_type = extract_leadster_signal(payload)
    enriched = {
        **payload,
        "email": first_value(payload, "email", "mail", "person_email"),
        "phone": normalize_phone(first_value(payload, "phone", "whatsapp", "telefone", "celular")),
        "source": "leadster",
    }
    result = apply_temperature_to_deal(
        int(payload.get("deal_id") or 0),
        signal_type=signal_type,
        payload=enriched,
        dry_run=not apply_enabled(),
    )
    return {"ok": True, "applied": apply_enabled(), "signal_type": signal_type, "result": result}


@router.post("/webhooks/mand-lp")
async def mand_lp_webhook(req: Request) -> dict[str, Any]:
    payload = await req.json()
    raw = " ".join(str(payload.get(key) or "") for key in ("event", "type", "source")).lower()
    signal_type = "mand_lp_form" if re.search(r"form|lead", raw) else "mand_lp_visit"
    enriched = {
        **payload,
        "email": first_value(payload, "email", "mail", "person_email"),
        "phone": normalize_phone(first_value(payload, "phone", "whatsapp", "telefone", "celular")),
        "source": "mand_lp",
    }
    result = apply_temperature_to_deal(
        int(payload.get("deal_id") or 0),
        signal_type=signal_type,
        payload=enriched,
        dry_run=not apply_enabled(),
    )
    return {"ok": True, "applied": apply_enabled(), "signal_type": signal_type, "result": result}
