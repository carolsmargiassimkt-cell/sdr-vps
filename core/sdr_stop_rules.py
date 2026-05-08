from __future__ import annotations

from typing import Any

from core.human_handoff import is_human_handoff, normalize_phone


STOP_TOKENS = {
    "responded",
    "respondido",
    "wa_respondeu",
    "conversando",
    "meeting_booked",
    "calendly_booked",
    "won",
    "lost",
    "opt_out",
    "negative_stop",
    "wrong_contact",
    "numero_errado",
    "sem_interesse",
}


def _tokens_from_labels(raw: Any) -> set[str]:
    if isinstance(raw, list):
        values = raw
    else:
        values = str(raw or "").split(",")
    out = set()
    for item in values:
        if isinstance(item, dict):
            for key in ("id", "name", "label", "value"):
                if item.get(key) is not None:
                    out.add(str(item.get(key)).strip().lower())
        else:
            token = str(item or "").strip().lower()
            if token:
                out.add(token)
    return out


def should_stop_all_automation(deal: dict[str, Any] | None = None, *, phone: Any = "") -> bool:
    phone_value = normalize_phone(phone or (deal or {}).get("phone"))
    if phone_value and is_human_handoff(phone_value):
        return True
    status = str((deal or {}).get("status") or "").strip().lower()
    if status in {"won", "lost"}:
        return True
    tokens = _tokens_from_labels((deal or {}).get("label") or (deal or {}).get("labels"))
    if tokens & {"196", "respondido", "wa_respondeu", "conversando"}:
        return True
    values = {
        str((deal or {}).get("status_bot") or "").strip().lower(),
        str((deal or {}).get("status_sdr") or "").strip().lower(),
        str((deal or {}).get("automation_status") or "").strip().lower(),
        str((deal or {}).get("meeting_status") or "").strip().lower(),
    }
    return bool((tokens | values) & STOP_TOKENS)

