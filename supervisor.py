from __future__ import annotations

import os
import random
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from core.whatsapp_hunter_guard import (
    duplicate_day_log,
    load_guard as load_hunter_daily_guard,
    mark_hunter_sent_today,
    render_hunter_message,
    should_send_hunter_today,
)
from core.wa_strategy_state import (
    append_sent_step,
    get_record,
    has_sent_step,
    load_strategy_state,
    save_strategy_state,
    set_record,
)
from core.sdr_orchestrator import (
    ACTION_MARK_LOST,
    ACTION_WA_HUNTER_SEND,
    load_email_state,
    resolve_next_action,
)
from crm.pipedrive_client import PipedriveClient
from services.whatsapp_service import WhatsAppService


PIPELINE_ID = int(os.getenv("PIPEDRIVE_PIPELINE_ID", "7") or 7)
DAILY_CAP = int(os.getenv("WA_HUNTER_DAILY_CAP", "20") or 20)
DRY_RUN = str(os.getenv("WA_HUNTER_DRY_RUN", "true") or "true").strip().lower() not in {"0", "false", "no", "off"}
MIN_DELAY_SEC = int(os.getenv("WA_HUNTER_MIN_DELAY_SEC", "900") or 900)
MAX_DELAY_SEC = int(os.getenv("WA_HUNTER_MAX_DELAY_SEC", "2700") or 2700)
GATEWAY_URL = os.getenv("WHATSAPP_GATEWAY_SEND_URL", "http://127.0.0.1:3000/send")

IMPEDITIVE_TAGS = {
    "CONVERSANDO",
    "RESPONDIDO",
    "LINKEDIN",
    "INDICACAO",
    "INDICAÇÃO",
    "CARO EVENTO",
    "CARO_EVENTO",
    "LEAD TRAFEGO",
    "LEAD_TRAFEGO",
    "LEAD_TRÁFEGO",
    "LEAD TRÁFEGO",
    "193",
    "196",
    "226",
    "WARM_WHATSAPP",
    "CLICKED_WARM",
}
WARM_STATUS_TOKENS = {
    "clicked_warm",
    "replied",
    "respondido",
    "conversando",
    "lead_interessado",
    "warm",
}
WARM_STAGE_KEYWORDS = {
    "warm",
    "reuniao",
    "reunião",
    "proposta",
    "qualificado",
    "conexao",
    "conexão",
}


crm = PipedriveClient()
whatsapp = WhatsAppService()


def is_brazil_business_hours():
    now = datetime.now(ZoneInfo("America/Sao_Paulo"))
    if now.weekday() >= 5:
        return False
    try:
        import holidays

        if now.date() in holidays.Brazil():
            return False
    except Exception:
        pass
    return 9 <= now.hour < 18


def normalize_phone(value):
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def phone_from_deal(deal):
    person = (deal or {}).get("person_id") or {}
    if isinstance(person, dict):
        for item in list(person.get("phone") or []):
            phone = normalize_phone((item or {}).get("value"))
            if phone:
                return phone
    return ""


def label_tokens(deal):
    try:
        return {
            str(token or "").strip().upper()
            for token in crm.resolve_label_tokens((deal or {}).get("label"), crm.get_deal_labels())
            if str(token or "").strip()
        }
    except Exception:
        raw = (deal or {}).get("label")
        if isinstance(raw, list):
            return {str(item.get("id") if isinstance(item, dict) else item).strip().upper() for item in raw if str(item or "").strip()}
        return {item.strip().upper() for item in str(raw or "").split(",") if item.strip()}


def blocklist_values():
    values = set()
    paths = [
        "data/whatsapp_manual_blocklist.json",
        "data/whatsapp_blocklist.json",
        "logs/whatsapp_manual_blocklist.json",
        "invalidos.json",
    ]
    import json
    from pathlib import Path

    def walk(payload):
        if isinstance(payload, dict):
            for key, value in payload.items():
                yield key
                yield from walk(value)
        elif isinstance(payload, list):
            for item in payload:
                yield from walk(item)
        else:
            yield payload

    for path in paths:
        p = Path(path)
        if not p.exists():
            continue
        try:
            payload = json.loads(p.read_text(encoding="utf-8-sig") or "[]")
        except Exception:
            continue
        for raw in walk(payload):
            phone = normalize_phone(raw)
            if phone:
                values.add(phone)
                if phone.startswith("55"):
                    values.add(phone[2:])
    return values


def deal_stage_name(deal):
    stage = (deal or {}).get("stage_id")
    if isinstance(stage, dict):
        return str(stage.get("name") or "").strip().lower()
    return str((deal or {}).get("stage_name") or "").strip().lower()


def build_hunter_message(deal, step):
    return render_hunter_message(deal, (deal or {}).get("person_id") if isinstance((deal or {}).get("person_id"), dict) else None, (deal or {}).get("org_id") if isinstance((deal or {}).get("org_id"), dict) else None, step=f"hunter_{int(step)}", phone=phone_from_deal(deal))


def is_warm_deal(deal, state, phone):
    deal_id = int((deal or {}).get("id") or 0)
    tokens = label_tokens(deal)
    status = str((deal or {}).get("status_bot") or (deal or {}).get(crm.STATUS_BOT_FIELD) or "").strip().lower()
    stage_name = deal_stage_name(deal)
    if tokens & {"226", "WARM_WHATSAPP", "CLICKED_WARM", "RESPONDIDO", "196", "CONVERSANDO"}:
        return True
    if status in WARM_STATUS_TOKENS:
        return True
    if any(keyword in stage_name for keyword in WARM_STAGE_KEYWORDS):
        return True
    warm_record = get_record(state, deal_id, phone, "wa_warm")
    return warm_record.get("status") in {"active", "sent", "warm"}


def hunter_skip_reason(deal, state, blocked):
    deal_id = int((deal or {}).get("id") or 0)
    if deal_id <= 0:
        return "invalid_deal"
    if str((deal or {}).get("status") or "open").lower() != "open":
        return "not_open"
    phone = phone_from_deal(deal)
    if not phone or not whatsapp.is_valid_phone(phone):
        return "invalid_phone"
    if phone in blocked or (phone.startswith("55") and phone[2:] in blocked):
        return "blocklist"
    tokens = label_tokens(deal)
    if tokens & IMPEDITIVE_TAGS:
        return "impeditive_tag"
    if is_warm_deal(deal, state, phone):
        return "warm"
    record = get_record(state, deal_id, phone, "wa_hunter")
    if record.get("status") in {"warm", "stopped", "blocked"}:
        return f"state_{record.get('status')}"
    sent_steps = {str(item) for item in list(record.get("sent_steps") or [])}
    if has_sent_step(state, deal_id, phone, "wa_hunter", "hunter_1") and int(record.get("hunter_step") or 0) <= 0:
        return "opening_already_sent"
    return ""


def next_hunter_step(state, deal_id, phone):
    record = get_record(state, deal_id, phone, "wa_hunter")
    sent = {str(item) for item in list(record.get("sent_steps") or [])}
    last_step = int(record.get("hunter_step") or 0)
    for historical_step in range(1, max(0, last_step) + 1):
        sent.add(f"hunter_{historical_step}")
    for step in (1, 2, 3):
        if f"hunter_{step}" not in sent:
            return step
    return 0


def phone_cooldown_active(state, phone, cooldown_sec=None):
    cooldown = int(cooldown_sec or MIN_DELAY_SEC)
    phone_value = normalize_phone(phone)
    now_ts = time.time()
    for record in list((state or {}).values()):
        if not isinstance(record, dict):
            continue
        if normalize_phone(record.get("phone")) != phone_value:
            continue
        raw = str(record.get("last_sent_at") or "").strip()
        if not raw:
            continue
        try:
            last_ts = datetime.fromisoformat(raw.replace("Z", "")).timestamp()
        except Exception:
            try:
                last_ts = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                continue
        if now_ts - last_ts < cooldown:
            return True
    return False


def load_open_deals(limit=500):
    return crm.get_deals(status="open", pipeline_id=PIPELINE_ID, limit=int(limit or 500))


def send_whatsapp(phone, text):
    response = requests.post(
        GATEWAY_URL,
        json={"number": phone, "text": text, "count_towards_daily_limit": True},
        timeout=60,
    )
    return response.ok and '"sent"' in response.text


def add_note(deal_id, content):
    try:
        ok = bool(crm.add_note(deal_id=int(deal_id), content=str(content or "").strip()))
    except Exception:
        ok = False
    print(f"[PIPEDRIVE_NOTE_ADD] deal_id={deal_id} ok={1 if ok else 0}")
    return ok


def mark_lost_after_hunter(deal_id):
    note = "[WA_HUNTER_DROP] Perdido/dropado por sem resposta apos cadencia WhatsApp Hunter 3/3."
    ok = False
    try:
        ok = bool(crm.update_deal(int(deal_id), {"status": "lost", "lost_reason": "Sem resposta apos cadencia WhatsApp Hunter"}))
    except Exception as exc:
        print(f"[ORCH_DROP_LOST_FAIL] deal_id={deal_id} error={exc}")
    add_note(deal_id, note)
    print(f"[ORCH_DROP_LOST] deal_id={deal_id} ok={1 if ok else 0}")
    return ok


def run_hunter(*, send=False, limit=None):
    dry_run = DRY_RUN or not send
    if not is_brazil_business_hours():
        print("[WA_HUNTER_SKIP] reason=outside_business_hours")
        return {"eligible": 0, "sent": 0}

    state = load_strategy_state()
    daily_guard = load_hunter_daily_guard()
    email_state = load_email_state()
    blocked = blocklist_values()
    deals = load_open_deals()
    daily_cap = min(int(limit or DAILY_CAP), DAILY_CAP)
    sent = 0
    eligible = 0

    for deal in deals:
        deal_id = int((deal or {}).get("id") or 0)
        phone = phone_from_deal(deal)
        reason = hunter_skip_reason(deal, state, blocked)
        if reason:
            print(f"[WA_HUNTER_SKIP] deal_id={deal_id} phone={phone or '-'} reason={reason}")
            if reason == "warm":
                set_record(state, deal_id, phone, "wa_hunter", status="warm", reason="warm_detected")
                print(f"[WA_HUNTER_STOP_WARM] deal_id={deal_id} phone={phone} reason=warm_detected")
            continue

        decision = resolve_next_action(
            deal,
            email_state=email_state,
            wa_state=state,
            channel="wa_hunter",
            phone=phone,
        )
        if decision.get("action") == ACTION_MARK_LOST:
            print(f"[ORCH_DROP_LOST] deal_id={deal_id} phone={phone} dry_run={1 if dry_run else 0} reason={decision.get('reason')}")
            if not dry_run:
                mark_lost_after_hunter(deal_id)
                set_record(state, deal_id, phone, "wa_hunter", status="completed", reason="dropped_lost")
                save_strategy_state(state)
            continue
        if decision.get("action") != ACTION_WA_HUNTER_SEND:
            print(f"[ORCH_BLOCK] channel=wa_hunter deal_id={deal_id} phone={phone} reason={decision.get('reason')}")
            continue

        step = int(decision.get("step") or next_hunter_step(state, deal_id, phone))
        if step <= 0:
            set_record(state, deal_id, phone, "wa_hunter", status="completed", reason="max_steps")
            print(f"[WA_HUNTER_SKIP] deal_id={deal_id} phone={phone} reason=max_steps")
            continue
        step_name = f"hunter_{step}"
        if has_sent_step(state, deal_id, phone, "wa_hunter", step_name):
            print(f"[WA_HUNTER_DUP_GUARD] deal_id={deal_id} phone={phone} step={step_name}")
            continue
        if phone_cooldown_active(state, phone):
            print(f"[WA_HUNTER_SKIP] deal_id={deal_id} phone={phone} reason=phone_cooldown")
            continue
        allowed_today, last_sent_at, duplicate_reason = should_send_hunter_today(deal_id, phone, daily_guard)
        if not allowed_today:
            print(duplicate_day_log(deal_id, phone, last_sent_at, duplicate_reason))
            continue

        eligible += 1
        print(f"[WA_HUNTER_ELIGIBLE] deal_id={deal_id} phone={phone} step={step_name} dry_run={1 if dry_run else 0}")
        if dry_run:
            continue
        if sent >= daily_cap:
            print(f"[WA_HUNTER_SKIP] reason=daily_cap cap={daily_cap}")
            break

        text = build_hunter_message(deal, step)
        ok = send_whatsapp(phone, text)
        if not ok:
            print(f"[WA_HUNTER_SKIP] deal_id={deal_id} phone={phone} reason=send_failed")
            continue
        sent_at = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
        daily_guard = mark_hunter_sent_today(deal_id, phone, sent_at=sent_at, guard=daily_guard)
        append_sent_step(
            state,
            deal_id,
            phone,
            "wa_hunter",
            step_name,
            hunter_step=step,
            last_sent_at=sent_at,
            status="active" if step < 3 else "completed",
            reason="sent",
        )
        add_note(deal_id, f"[WA_HUNTER_{step}] WhatsApp hunter etapa {step}/3 enviada.")
        print(f"[WA_HUNTER_SENT] deal_id={deal_id} phone={phone} step={step_name}")
        save_strategy_state(state)
        sent += 1
        time.sleep(random.randint(MIN_DELAY_SEC, MAX_DELAY_SEC))

    save_strategy_state(state)
    return {"eligible": eligible, "sent": sent}


def main() -> int:
    send = "--send" in sys.argv and not DRY_RUN
    if "--send" in sys.argv and DRY_RUN:
        print("[WA_HUNTER_SKIP] reason=dry_run_env_true set WA_HUNTER_DRY_RUN=false to send")
    result = run_hunter(send=send)
    print(f"[WA_HUNTER_SUMMARY] eligible={result['eligible']} sent={result['sent']} dry_run={1 if (DRY_RUN or not send) else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
