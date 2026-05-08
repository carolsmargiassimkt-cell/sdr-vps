from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.locked_json_state import locked_append_jsonl, locked_load_json, locked_save_json
from core.production_safety import _locked_file
from core.sdr_orchestrator import load_email_state, load_wa_state, save_email_state, save_wa_state, stop_cold_states_for_warm
from crm.pipedrive_client import PipedriveClient


BASE_DIR = Path(__file__).resolve().parents[1]
EVENTS_FILE = BASE_DIR / "data" / "sdr_signal_events.jsonl"
MEETING_STATE_FILE = BASE_DIR / "data" / "sdr_meeting_reminders.json"
try:
    TZ = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ = timezone(timedelta(hours=-3))
STAGE_PRONTO_PROSPECCAO = 63

TEMPERATURE_BY_SIGNAL = {
    "email_open": "MORNO_LEVE",
    "email_click": "MORNO_FORTE",
    "leadster_visit": "MORNO",
    "mand_lp_visit": "MORNO",
    "leadster_form": "QUENTE",
    "leadster_chat_qualified": "QUENTE",
    "mand_lp_form": "QUENTE",
    "forms_lead": "QUENTE",
    "calendly_booked": "QUENTE_PRIORIDADE",
    "invitee.created": "QUENTE_PRIORIDADE",
    "calendly_canceled": "CANCELADO",
    "invitee.canceled": "CANCELADO",
}

STOP_COLD_TEMPERATURES = {"MORNO_FORTE", "QUENTE", "QUENTE_PRIORIDADE"}
ACTIVITY_TEMPERATURES = {"MORNO", "QUENTE", "QUENTE_PRIORIDADE"}
FIELD_LABELS = {
    "Temperatura SDR": "temperature",
    "Origem do sinal": "source",
    "Último sinal SDR": "signal_type",
    "Ultimo sinal SDR": "signal_type",
    "Data último sinal SDR": "signal_at",
    "Data ultimo sinal SDR": "signal_at",
}

MEETING_REMINDER_TEMPLATES = {
    "d_minus_1": [
        "Oi, tudo bem? Passando só para confirmar nossa reunião de amanhã às {time}.",
        "Olá! Tudo certo para nossa conversa amanhã às {time}?",
        "Oi! Só confirmando nossa reunião de amanhã às {time}. Te espero por lá.",
    ],
    "d0": [
        "Oi, tudo bem? Confirmando nossa reunião de hoje às {time}.",
        "Olá! Passando para lembrar da nossa conversa hoje às {time}.",
        "Oi! Nossa reunião é hoje às {time}. Tudo certo por aí?",
    ],
}


def now_brt() -> datetime:
    return datetime.now(TZ)


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_signal_source(signal_type: str | None = None, payload: dict[str, Any] | None = None) -> str:
    body = payload or {}
    raw = str(body.get("source") or body.get("utm_source") or signal_type or "").strip().lower()
    if "calendly" in raw or str(signal_type or "").startswith("invitee."):
        return "calendly"
    if "leadster" in raw:
        return "leadster"
    if "mand" in raw or "lp" in raw:
        return "mand_lp"
    if "email" in raw:
        return "email_cadence"
    if "form" in raw:
        return "forms"
    return raw or "unknown"


def _stable_json(payload: Any) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return str(payload or "")


def _payload_path(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def extract_calendly(payload: dict[str, Any]) -> dict[str, Any]:
    event = str(payload.get("event") or payload.get("type") or "").strip()
    data = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    questions = data.get("questions_and_answers") if isinstance(data, dict) else []
    phone = data.get("text_reminder_number") if isinstance(data, dict) else ""
    if not phone and isinstance(questions, list):
        for item in questions:
            answer = str((item or {}).get("answer") or "")
            if re.search(r"\d{10,}", answer):
                phone = answer
                break
    scheduled = data.get("scheduled_event") if isinstance(data, dict) else {}
    return {
        "event": event,
        "name": data.get("name") or data.get("first_name") or "",
        "email": normalize_email(data.get("email")),
        "phone": normalize_phone(phone),
        "start_time": scheduled.get("start_time") or data.get("start_time") or "",
        "end_time": scheduled.get("end_time") or data.get("end_time") or "",
        "event_uri": scheduled.get("uri") or data.get("event") or "",
        "invitee_uri": data.get("uri") or "",
        "rescheduled": bool(data.get("rescheduled") or data.get("new_invitee")),
        "cancel_reason": data.get("cancellation", {}).get("reason") if isinstance(data.get("cancellation"), dict) else "",
    }


def classify_signal(signal_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    raw_type = str(signal_type or "").strip()
    normalized = {
        "invitee.created": "calendly_booked",
        "invitee.canceled": "calendly_canceled",
    }.get(raw_type, raw_type)
    payload = payload or {}
    source = normalize_signal_source(normalized, payload)
    temperature = TEMPERATURE_BY_SIGNAL.get(normalized, "MORNO")
    external_id = (
        str(payload.get("external_id") or "")
        or str(payload.get("invitee_uri") or "")
        or str(payload.get("event_uri") or "")
        or str(payload.get("uri") or "")
    )
    if source == "calendly":
        calendly = extract_calendly(payload)
        external_id = external_id or calendly.get("invitee_uri") or calendly.get("event_uri")
    dedupe_seed = "|".join([normalized, external_id, str(payload.get("deal_id") or ""), _stable_json(payload)[:500]])
    dedupe_key = hashlib.sha256(dedupe_seed.encode("utf-8")).hexdigest()
    return {
        "signal_type": normalized,
        "temperature": temperature,
        "source": source,
        "external_id": external_id,
        "dedupe_key": dedupe_key,
    }


def should_stop_cold_cadence(temperature: str) -> bool:
    return str(temperature or "").strip().upper() in STOP_COLD_TEMPERATURES


def should_create_activity(temperature: str) -> bool:
    return str(temperature or "").strip().upper() in ACTIVITY_TEMPERATURES


def load_meeting_state() -> dict[str, Any]:
    payload = locked_load_json(MEETING_STATE_FILE, {})
    return payload if isinstance(payload, dict) else {}


def save_meeting_state(state: dict[str, Any]) -> None:
    locked_save_json(MEETING_STATE_FILE, state if isinstance(state, dict) else {})


def event_exists(dedupe_key: str) -> bool:
    if not EVENTS_FILE.exists():
        return False
    try:
        print(f"[STATE_LOCK_ACQUIRE] path={EVENTS_FILE}")
        with _locked_file(str(EVENTS_FILE) + ".lock"):
            with EVENTS_FILE.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    if (json.loads(line).get("dedupe_key") or "") == dedupe_key:
                        return True
    except Exception as exc:
        print(f"[SDR_SIGNAL_EVENT_READ_FAIL] error={exc}")
    finally:
        print(f"[STATE_LOCK_RELEASE] path={EVENTS_FILE}")
    return False


def append_event(event: dict[str, Any]) -> None:
    locked_append_jsonl(EVENTS_FILE, event)


def deal_field_update_payload(crm: PipedriveClient, classification: dict[str, Any]) -> dict[str, Any]:
    wanted = {key.lower(): value for key, value in FIELD_LABELS.items()}
    payload: dict[str, Any] = {}
    try:
        fields = crm.get_deal_fields()
    except Exception:
        fields = []
    for field in list(fields or []):
        name = str(field.get("name") or "").strip()
        target = wanted.get(name.lower())
        if not target:
            continue
        key = str(field.get("key") or "").strip()
        if not key:
            continue
        if target == "temperature":
            payload[key] = classification.get("temperature")
        elif target == "source":
            payload[key] = classification.get("source")
        elif target == "signal_type":
            payload[key] = classification.get("signal_type")
        elif target == "signal_at":
            payload[key] = now_brt().isoformat(timespec="seconds")
    return payload


def _extract_id(value: Any) -> int:
    if isinstance(value, dict):
        value = value.get("value") or value.get("id")
    try:
        return int(value or 0)
    except Exception:
        return 0


def find_crm_context(email: str = "", phone: str = "", deal_id: int = 0) -> dict[str, Any]:
    crm = PipedriveClient()
    deal = crm.get_deal_details(int(deal_id)) if int(deal_id or 0) else {}
    person = {}
    if not deal:
        person = crm.find_person(email=email, phone=phone)
        person_id = _extract_id(person.get("id"))
        deals = crm.find_open_deals_by_person_or_org(person_id=person_id) if person_id else []
        deal = dict(deals[0]) if deals else {}
    person_id = _extract_id((deal or {}).get("person_id")) or _extract_id(person.get("id"))
    org_id = _extract_id((deal or {}).get("org_id")) or _extract_id(person.get("org_id"))
    return {"crm": crm, "deal": deal or {}, "deal_id": _extract_id((deal or {}).get("id")) or int(deal_id or 0), "person_id": person_id, "org_id": org_id}


def has_pending_activity(crm: PipedriveClient, *, deal_id: int, activity_type: str = "", subject_contains: str = "", future_only: bool = False) -> bool:
    target_deal_id = int(deal_id or 0)
    if target_deal_id <= 0:
        return False
    try:
        activities = crm.get_activities(deal_id=target_deal_id, done=0, limit=100)
    except Exception:
        activities = []
    today = now_brt().date().isoformat()
    type_filter = str(activity_type or "").strip().lower()
    subject_filter = str(subject_contains or "").strip().lower()
    for activity in list(activities or []):
        if not isinstance(activity, dict):
            continue
        if _extract_id(activity.get("deal_id")) != target_deal_id:
            continue
        if type_filter and str(activity.get("type") or "").strip().lower() != type_filter:
            continue
        if subject_filter and subject_filter not in str(activity.get("subject") or "").strip().lower():
            continue
        due_date_value = str(activity.get("due_date") or "").strip()
        if future_only and due_date_value and due_date_value < today:
            continue
        return True
    return False


def create_activity_once(crm: PipedriveClient, *, deal_id: int, person_id: int = 0, org_id: int = 0, subject: str, activity_type: str, due_date: str, due_time: str = "", note: str = "", dry_run: bool = True) -> bool:
    if has_pending_activity(crm, deal_id=deal_id, activity_type=activity_type, subject_contains=subject):
        print(f"[CALL_ACTIVITY_SKIP_ALREADY_EXISTS] deal_id={deal_id} type={activity_type} subject={subject}")
        return False
    if dry_run:
        print(f"[SDR_TEMPERATURE_DRY_RUN] create_activity deal_id={deal_id} subject={subject} due_date={due_date} due_time={due_time}")
        return True
    return bool(crm.create_activity(subject=subject, type=activity_type, deal_id=int(deal_id or 0), person_id=int(person_id or 0), org_id=int(org_id or 0), due_date=due_date, due_time=due_time or None, note=note, done=0))


def ensure_pronto_call_activity(deal: dict[str, Any], *, dry_run: bool = True) -> bool:
    deal_id = _extract_id((deal or {}).get("id"))
    if deal_id <= 0:
        return False
    if str((deal or {}).get("status") or "open").strip().lower() in {"won", "lost", "stopped"}:
        return False
    if _extract_id((deal or {}).get("stage_id")) != STAGE_PRONTO_PROSPECCAO:
        return False
    current = now_brt()
    if current.weekday() >= 5 or not (9 <= current.hour < 18):
        return False
    crm = PipedriveClient()
    if has_pending_activity(crm, deal_id=deal_id, activity_type="call", future_only=True):
        print(f"[CALL_ACTIVITY_SKIP_ALREADY_EXISTS] deal_id={deal_id}")
        return False
    person_id = _extract_id((deal or {}).get("person_id"))
    org_id = _extract_id((deal or {}).get("org_id"))
    title = str((deal or {}).get("title") or f"deal {deal_id}")
    origin = str((deal or {}).get("origin") or (deal or {}).get("source") or "-")
    note = (
        "Lead pronto para prospecção.<br><br>"
        "Sinais detectados:<br>"
        f"- origem do lead: {origin}<br>"
        f"- warm score: {(deal or {}).get('lead_score') or '-'}<br>"
        f"- email click/open: {(deal or {}).get('email_clicked') or '-'}<br>"
        f"- formulário preenchido: {(deal or {}).get('form_responded') or '-'}<br>"
        f"- LinkedIn/Waalaxy: {(deal or {}).get('linkedin') or (deal or {}).get('waalaxy') or '-'}<br>"
        f"- WhatsApp warm: {(deal or {}).get('warm_source') or '-'}<br>"
        f"- segmento: {(deal or {}).get('segmento') or '-'}<br><br>"
        "Objetivo da call:<br>"
        "- validar decisor<br>"
        "- validar telefone/WhatsApp<br>"
        "- abrir conversa comercial"
    )
    due = current + timedelta(hours=1)
    ok = create_activity_once(
        crm,
        deal_id=deal_id,
        person_id=person_id,
        org_id=org_id,
        subject=f"Ligar - Lead pronto para prospecção - {title}",
        activity_type="call",
        due_date=due.date().isoformat(),
        due_time=due.strftime("%H:%M"),
        note=note,
        dry_run=dry_run,
    )
    if ok:
        print(f"[CALL_ACTIVITY_CREATED] deal_id={deal_id} dry_run={1 if dry_run else 0}")
        if not dry_run:
            crm.add_note(deal_id=deal_id, content=note)
    return ok


def stop_cold_for_deal(deal_id: int, phone: str, dry_run: bool) -> None:
    email_rows = load_email_state()
    wa_state = load_wa_state()
    email_changed, wa_changed = stop_cold_states_for_warm(int(deal_id), phone, email_rows, wa_state, reason="temperature_signal")
    if dry_run:
        print(f"[SDR_TEMPERATURE_DRY_RUN] stop_cold deal_id={deal_id} email={1 if email_changed else 0} wa={1 if wa_changed else 0}")
        return
    if email_changed:
        save_email_state(email_rows)
    if wa_changed:
        save_wa_state(wa_state)


def register_meeting_reminder_state(deal_id: int, phone: str, calendly: dict[str, Any], *, canceled: bool = False, dry_run: bool = True) -> None:
    due_date, due_time = _activity_date_time(calendly.get("start_time"))
    print("[MEETING_REMINDER_QUEUE]", f"deal_id={deal_id}", f"date={due_date}", f"time={due_time}", f"dry_run={1 if dry_run else 0}")
    if dry_run:
        return
    state = load_meeting_state()
    state[str(int(deal_id or 0))] = {
        "deal_id": int(deal_id or 0),
        "phone": normalize_phone(phone or calendly.get("phone")),
        "status": "cancelled" if canceled else "booked",
        "meeting_date": due_date,
        "meeting_time": due_time,
        "event_uri": calendly.get("event_uri") or "",
        "invitee_uri": calendly.get("invitee_uri") or "",
        "updated_at": now_brt().isoformat(timespec="seconds"),
        "reminders": {"d_minus_1": "queued", "d0": "queued"} if not canceled else {},
    }
    save_meeting_state(state)


def meeting_reminder_decision(record: dict[str, Any], reminder_key: str, *, current_date: str | None = None) -> dict[str, Any]:
    deal_id = int((record or {}).get("deal_id") or 0)
    status = str((record or {}).get("status") or "").strip().lower()
    meeting_date = str((record or {}).get("meeting_date") or "").strip()
    current = current_date or now_brt().date().isoformat()
    if status == "cancelled":
        print("[MEETING_REMINDER_SKIP_CANCELLED]", f"deal_id={deal_id}")
        return {"action": "skip", "reason": "cancelled"}
    if status == "confirmed":
        print("[MEETING_REMINDER_SKIP_CONFIRMED]", f"deal_id={deal_id}")
        return {"action": "skip", "reason": "confirmed"}
    if meeting_date and meeting_date < current:
        print("[MEETING_REMINDER_SKIP_PAST]", f"deal_id={deal_id}", f"meeting_date={meeting_date}")
        return {"action": "skip", "reason": "past"}
    template_options = MEETING_REMINDER_TEMPLATES.get(str(reminder_key or "d0"), MEETING_REMINDER_TEMPLATES["d0"])
    message = template_options[0].replace("{time}", str((record or {}).get("meeting_time") or "o horário combinado"))
    return {"action": "send", "reason": "due", "message": message}


def _activity_date_time(start_time: str) -> tuple[str, str]:
    parsed = None
    raw = str(start_time or "").strip()
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(TZ)
        except Exception:
            parsed = None
    parsed = parsed or now_brt()
    return parsed.date().isoformat(), parsed.strftime("%H:%M")


def apply_temperature_to_deal(deal_id: int, person_id: int | None = None, org_id: int | None = None, signal_type: str | None = None, payload: dict[str, Any] | None = None, dry_run: bool = True) -> dict[str, Any]:
    payload = payload or {}
    classification = classify_signal(signal_type or payload.get("signal_type") or "", payload)
    context = find_crm_context(payload.get("email") or "", payload.get("phone") or "", int(deal_id or payload.get("deal_id") or 0))
    crm: PipedriveClient = context["crm"]
    resolved_deal_id = int(context.get("deal_id") or deal_id or 0)
    resolved_person_id = int(person_id or context.get("person_id") or 0)
    resolved_org_id = int(org_id or context.get("org_id") or 0)
    calendly = extract_calendly(payload) if classification["source"] == "calendly" else {}
    phone = normalize_phone(payload.get("phone") or calendly.get("phone"))

    event = {
        "ts": now_brt().isoformat(timespec="seconds"),
        "deal_id": resolved_deal_id,
        "person_id": resolved_person_id,
        "org_id": resolved_org_id,
        "signal_type": classification["signal_type"],
        "temperature": classification["temperature"],
        "source": classification["source"],
        "external_id": classification["external_id"],
        "dedupe_key": classification["dedupe_key"],
    }
    if event_exists(event["dedupe_key"]):
        print(f"[SDR_SIGNAL_DUP] dedupe_key={event['dedupe_key']} signal_type={event['signal_type']}")
        return {"ok": True, "duplicate": True, **event}
    append_event(event)

    if not resolved_deal_id:
        print(f"[SDR_SIGNAL_ORPHAN] signal_type={event['signal_type']} email={payload.get('email') or calendly.get('email')} phone={phone}")
        return {"ok": True, "orphan": True, **event}

    note = f"[SDR_SIGNAL] temperature={event['temperature']} source={event['source']} signal_type={event['signal_type']} external_id={event['external_id'] or '-'}"
    updates = deal_field_update_payload(crm, classification)
    if dry_run:
        print(f"[SDR_TEMPERATURE_DRY_RUN] deal_id={resolved_deal_id} note={note} updates={updates}")
    else:
        try:
            crm.add_note(deal_id=resolved_deal_id, content=note)
            if updates:
                crm.update_deal(resolved_deal_id, updates)
        except Exception as exc:
            print(f"[SDR_TEMPERATURE_PD_FAIL] deal_id={resolved_deal_id} error={exc}")

    if should_stop_cold_cadence(event["temperature"]):
        stop_cold_for_deal(resolved_deal_id, phone, dry_run=dry_run)

    if event["signal_type"] == "calendly_booked":
        due_date, due_time = _activity_date_time(calendly.get("start_time"))
        title = calendly.get("name") or payload.get("name") or f"deal {resolved_deal_id}"
        register_meeting_reminder_state(resolved_deal_id, phone, calendly, dry_run=dry_run)
        create_activity_once(crm, deal_id=resolved_deal_id, person_id=resolved_person_id, org_id=resolved_org_id, subject=f"Reunião Calendly - {title}", activity_type="meeting", due_date=due_date, due_time=due_time, note=note, dry_run=dry_run)
        meeting_date = datetime.fromisoformat(f"{due_date}T00:00:00")
        create_activity_once(crm, deal_id=resolved_deal_id, person_id=resolved_person_id, org_id=resolved_org_id, subject=f"Lembrete reunião amanhã - {title}", activity_type="task", due_date=(meeting_date - timedelta(days=1)).date().isoformat(), note=note, dry_run=dry_run)
        create_activity_once(crm, deal_id=resolved_deal_id, person_id=resolved_person_id, org_id=resolved_org_id, subject=f"Confirmar reunião hoje - {title}", activity_type="task", due_date=due_date, note=note, dry_run=dry_run)
    elif event["signal_type"] == "calendly_canceled":
        register_meeting_reminder_state(resolved_deal_id, phone, calendly, canceled=True, dry_run=dry_run)
        if dry_run:
            print(f"[SDR_TEMPERATURE_DRY_RUN] calendly_canceled deal_id={resolved_deal_id} rescheduled={calendly.get('rescheduled')}")
        else:
            crm.add_note(deal_id=resolved_deal_id, content=f"[SDR_SIGNAL] Calendly cancelado. rescheduled={calendly.get('rescheduled')} reason={calendly.get('cancel_reason') or '-'}")
            if not calendly.get("rescheduled"):
                create_activity_once(crm, deal_id=resolved_deal_id, person_id=resolved_person_id, org_id=resolved_org_id, subject="Reagendar reunião Calendly", activity_type="call", due_date=now_brt().date().isoformat(), note="Calendly cancelado sem remarcação automática.", dry_run=False)
    elif should_create_activity(event["temperature"]) and event["temperature"] in {"MORNO", "QUENTE"}:
        create_activity_once(crm, deal_id=resolved_deal_id, person_id=resolved_person_id, org_id=resolved_org_id, subject=f"Contato SDR - {event['temperature']} - {event['source']}", activity_type="call", due_date=now_brt().date().isoformat(), note=note, dry_run=dry_run)

    return {"ok": True, "duplicate": False, **event}
