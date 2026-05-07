from __future__ import annotations

import hashlib
import json
import os
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parents[1]
GUARD_FILE = Path(os.getenv("WA_HUNTER_DAILY_GUARD_FILE") or BASE_DIR / "data" / "whatsapp_hunter_daily_guard.json")
TZ = ZoneInfo("America/Sao_Paulo")


def now_brt() -> datetime:
    return datetime.now(TZ)


def today_brt() -> str:
    return now_brt().date().isoformat()


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def _empty_guard() -> dict[str, Any]:
    return {"date": today_brt(), "sent_by_phone": {}, "sent_by_deal": {}}


def load_guard() -> dict[str, Any]:
    try:
        if GUARD_FILE.exists():
            payload = json.loads(GUARD_FILE.read_text(encoding="utf-8-sig") or "{}")
            if isinstance(payload, dict) and payload.get("date") == today_brt():
                payload.setdefault("sent_by_phone", {})
                payload.setdefault("sent_by_deal", {})
                return payload
    except Exception as exc:
        print(f"[WA_HUNTER_GUARD_READ_FAIL] error={exc}")
    return _empty_guard()


def save_guard(guard: dict[str, Any]) -> None:
    GUARD_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = guard if isinstance(guard, dict) else _empty_guard()
    payload.setdefault("date", today_brt())
    payload.setdefault("sent_by_phone", {})
    payload.setdefault("sent_by_deal", {})
    tmp = GUARD_FILE.with_suffix(GUARD_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, GUARD_FILE)


def should_send_hunter_today(deal_id: Any, phone: Any, guard: dict[str, Any] | None = None) -> tuple[bool, str, str]:
    state = guard if isinstance(guard, dict) else load_guard()
    phone_value = normalize_phone(phone)
    deal_key = str(int(deal_id or 0)) if str(deal_id or "").strip() else "0"
    by_phone = state.get("sent_by_phone") if isinstance(state.get("sent_by_phone"), dict) else {}
    by_deal = state.get("sent_by_deal") if isinstance(state.get("sent_by_deal"), dict) else {}
    phone_last = by_phone.get(phone_value) if phone_value else ""
    deal_last = by_deal.get(deal_key) if deal_key else ""
    if phone_last:
        return False, str(phone_last), "already_sent_today_phone"
    if deal_last:
        return False, str(deal_last), "already_sent_today_deal"
    return True, "", ""


def mark_hunter_sent_today(deal_id: Any, phone: Any, sent_at: str | None = None, guard: dict[str, Any] | None = None, *, persist: bool = True) -> dict[str, Any]:
    state = guard if isinstance(guard, dict) else load_guard()
    if state.get("date") != today_brt():
        state = _empty_guard()
    timestamp = sent_at or now_brt().isoformat(timespec="seconds")
    phone_value = normalize_phone(phone)
    deal_key = str(int(deal_id or 0)) if str(deal_id or "").strip() else "0"
    state.setdefault("sent_by_phone", {})
    state.setdefault("sent_by_deal", {})
    if phone_value:
        state["sent_by_phone"][phone_value] = timestamp
    if deal_key:
        state["sent_by_deal"][deal_key] = timestamp
    if persist:
        save_guard(state)
    return state


def duplicate_day_log(deal_id: Any, phone: Any, last_sent_at: str, reason: str) -> str:
    phone_value = normalize_phone(phone)
    return (
        f"[WA_DUPLICATE_DAY_BLOCK] deal_id={int(deal_id or 0)} "
        f"phone={phone_value} last_sent_at={last_sent_at or '-'} "
        f"reason=already_sent_today source={reason}"
    )


def _rng_for(deal_id: Any, phone: Any, date_value: str, step: Any) -> random.Random:
    seed = f"{int(deal_id or 0)}:{normalize_phone(phone)}:{date_value}:{step}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return random.Random(int(digest[:16], 16))


def _company_name(deal: dict[str, Any] | None, org: dict[str, Any] | None = None) -> str:
    for source in (org or {}, (deal or {}).get("org_id") if isinstance((deal or {}).get("org_id"), dict) else {}):
        name = (source or {}).get("name") if isinstance(source, dict) else ""
        if str(name or "").strip():
            return str(name).strip()
    title = str((deal or {}).get("title") or "").strip()
    return title or "sua empresa"


def render_hunter_message(
    deal: dict[str, Any] | None = None,
    person: dict[str, Any] | None = None,
    org: dict[str, Any] | None = None,
    *,
    step: str | int = "hunter_1",
    phone: Any = "",
    date_value: str | None = None,
) -> str:
    deal_id = int((deal or {}).get("id") or (deal or {}).get("deal_id") or 0)
    step_value = str(step or "hunter_1")
    date_key = date_value or today_brt()
    rng = _rng_for(deal_id, phone, date_key, step_value)
    empresa = _company_name(deal, org)

    greetings = ["Oi, tudo bem?", "Olá, tudo certo?", "Bom dia, tudo bem?", "Boa tarde, tudo bem?"]
    intros = ["Aqui é a Carol da Mand Digital.", "Sou a Carol, da Mand Digital.", "Carol da Mand Digital por aqui."]
    asks = [
        "Queria falar com quem cuida de marketing ou campanhas promocionais por aí.",
        "Você sabe quem vê campanhas promocionais ou ações de marketing por aí?",
        f"Queria entender quem olha campanhas promocionais na {empresa}.",
        f"Sabe quem cuida de ações de marketing e campanhas na {empresa}?",
    ]
    value_hooks = [
        "A Mand ajuda a transformar campanhas em experiências com QR Code, roleta ou raspadinha para captar dados próprios.",
        "A gente estrutura campanhas de PDV e Copa com participação via QR Code, WhatsApp e captura de dados.",
        "A ideia é usar campanha promocional para gerar participação, base própria e remarketing depois.",
    ]
    closings = [
        "Faz sentido eu falar com marketing?",
        "Consegue me apontar a pessoa certa?",
        "Quem seria a melhor pessoa para eu chamar?",
    ]

    if step_value in {"1", "hunter_1"}:
        parts = [rng.choice(greetings), rng.choice(intros), rng.choice(asks)]
    elif step_value in {"2", "hunter_2"}:
        parts = [rng.choice(["Passando rapidinho por aqui.", "Carol da Mand novamente por aqui."]), rng.choice(value_hooks), rng.choice(closings)]
    else:
        parts = [rng.choice(["Última tentativa por aqui para não insistir.", "Prometo não insistir depois desta."]), rng.choice(value_hooks), "Se fizer sentido, fico à disposição."]
    message = " ".join(part.strip() for part in parts if part and part.strip())
    return message.replace("{", "").replace("}", "").replace("|", "")
