from __future__ import annotations

import argparse
import os
import smtplib
import ssl
import sys
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import requests

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.locked_json_state import locked_load_json, locked_save_json
from crm.pipedrive_client import PipedriveClient


WATCH_FILE = ROOT / "data" / "wa_warm_delivery_watch.json"
GATEWAY_URL = os.getenv("WA_GATEWAY_URL", "http://127.0.0.1:3000").rstrip("/")
PENDING_AFTER_MINUTES = int(os.getenv("WA_WARM_PENDING_FALLBACK_MINUTES", "10") or 10)

GABRIEL_SEED = {
    "deal_id": 3419,
    "person_id": 5952,
    "org_id": 2460,
    "phone": "5541999193626",
    "message_id": "3EB07EED182522F3D35DAE",
    "status": "PENDING",
    "source": "leadster_import",
    "fallback_done": False,
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return now_utc().isoformat(timespec="seconds")


def parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw[:19], fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def normalize_state(payload: Any) -> dict[str, dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            return {str(row.get("message_id") or ""): dict(row) for row in payload["items"] if row.get("message_id")}
        return {str(key): dict(value) for key, value in payload.items() if isinstance(value, dict)}
    if isinstance(payload, list):
        return {str(row.get("message_id") or ""): dict(row) for row in payload if isinstance(row, dict) and row.get("message_id")}
    return {}


def load_state() -> dict[str, dict[str, Any]]:
    return normalize_state(locked_load_json(WATCH_FILE, {}))


def save_state(state: dict[str, dict[str, Any]]) -> None:
    locked_save_json(WATCH_FILE, state)


def add_seed_gabriel(state: dict[str, dict[str, Any]]) -> bool:
    key = GABRIEL_SEED["message_id"]
    previous = state.get(key) or {}
    sent_at = previous.get("sent_at") or (now_utc() - timedelta(minutes=PENDING_AFTER_MINUTES + 5)).isoformat(timespec="seconds")
    state[key] = {
        **previous,
        **GABRIEL_SEED,
        "sent_at": sent_at,
        "updated_at": iso_now(),
    }
    print(f"[WA_WARM_DELIVERY_SEED] deal_id=3419 phone=5541999193626 message_id={key}")
    return True


def gateway_status(message_id: str) -> str:
    try:
        response = requests.get(f"{GATEWAY_URL}/outbound-status", params={"message_id": message_id}, timeout=10)
        if response.status_code == 404:
            return "NOT_FOUND"
        if response.status_code >= 400:
            print(f"[WA_WARM_STATUS_ERROR] message_id={message_id} status={response.status_code} body={response.text[:160]}")
            return "UNKNOWN"
        payload = response.json() or {}
        return str(payload.get("status") or "UNKNOWN").strip().upper()
    except Exception as exc:
        print(f"[WA_WARM_STATUS_EXCEPTION] message_id={message_id} error={exc}")
        return "UNKNOWN"


def extract_email(person: dict[str, Any]) -> str:
    values = person.get("email")
    if isinstance(values, list):
        for item in values:
            if isinstance(item, dict) and str(item.get("value") or "").strip():
                return str(item.get("value")).strip()
            if isinstance(item, str) and item.strip():
                return item.strip()
    if isinstance(values, str):
        return values.strip()
    return ""


def has_open_activity(crm: PipedriveClient, deal_id: int, subject: str) -> bool:
    try:
        activities = crm.get_activities(deal_id=int(deal_id), done=0, limit=100)
    except Exception:
        activities = []
    target = subject.strip().lower()
    for activity in activities or []:
        if target in str(activity.get("subject") or "").strip().lower():
            return True
    return False


def create_call_activity_once(crm: PipedriveClient, row: dict[str, Any], subject: str, *, apply: bool) -> bool:
    deal_id = int(row.get("deal_id") or 0)
    if deal_id <= 0:
        return False
    if has_open_activity(crm, deal_id, subject):
        print(f"[WA_WARM_FALLBACK_ACTIVITY_SKIP_DUP] deal_id={deal_id} subject={subject}")
        return False
    if not apply:
        print(f"[WA_WARM_FALLBACK_DRY_RUN] create_activity deal_id={deal_id} subject={subject}")
        return True
    due = datetime.now().date().isoformat()
    ok = bool(crm.create_activity(
        subject=subject,
        type="call",
        deal_id=deal_id,
        person_id=int(row.get("person_id") or 0),
        org_id=int(row.get("org_id") or 0),
        due_date=due,
        note="Fallback automático: WhatsApp warm ficou PENDING sem ACK/DELIVERED.",
        done=0,
    ))
    print(f"[WA_WARM_FALLBACK_ACTIVITY_CREATED] deal_id={deal_id} ok={1 if ok else 0}")
    return ok


def add_note(crm: PipedriveClient, row: dict[str, Any], content: str, *, apply: bool) -> bool:
    deal_id = int(row.get("deal_id") or 0)
    if deal_id <= 0:
        return False
    if not apply:
        print(f"[WA_WARM_FALLBACK_DRY_RUN] add_note deal_id={deal_id} content={content[:80]}")
        return True
    ok = bool(crm.add_note(deal_id=deal_id, content=content))
    print(f"[WA_WARM_FALLBACK_NOTE] deal_id={deal_id} ok={1 if ok else 0}")
    return ok


def send_contextual_email(to_addr: str, row: dict[str, Any], *, apply: bool) -> bool:
    if not to_addr:
        return False
    subject = "Sobre sua solicitação na Mand Digital"
    body = (
        "Oi, tudo bem?<br><br>"
        "Recebemos sua solicitação na Mand Digital e também tentamos te chamar pelo WhatsApp, "
        "mas a mensagem ainda não confirmou entrega por lá.<br><br>"
        "Vou deixar o contato registrado por aqui também. Se fizer sentido, posso te ajudar com o próximo passo."
    )
    if not apply:
        print(f"[WA_WARM_FALLBACK_EMAIL_DRY_RUN] to={to_addr} deal_id={row.get('deal_id')}")
        return True
    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "465") or 465)
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    from_addr = os.getenv("SMTP_FROM_EMAIL", "") or smtp_user
    if not all([smtp_host, smtp_user, smtp_pass, from_addr]):
        print(f"[WA_WARM_FALLBACK_EMAIL_SKIP] reason=smtp_env_missing to={to_addr}")
        return False
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body.replace("<br>", "\n"))
    msg.add_alternative(body, subtype="html")
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ssl.create_default_context(), timeout=30) as server:
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
    print(f"[WA_WARM_FALLBACK_EMAIL_SENT] to={to_addr} deal_id={row.get('deal_id')}")
    return True


def process_row(crm: PipedriveClient, key: str, row: dict[str, Any], *, apply: bool) -> bool:
    if bool(row.get("fallback_done")):
        return False
    status = str(row.get("status") or "PENDING").upper()
    if status not in {"PENDING", "UNKNOWN", "NOT_FOUND"}:
        return False
    sent_at = parse_dt(row.get("sent_at"))
    if sent_at and now_utc() - sent_at < timedelta(minutes=PENDING_AFTER_MINUTES):
        return False
    message_id = str(row.get("message_id") or key).strip()
    current_status = gateway_status(message_id)
    row["last_checked_at"] = iso_now()
    row["gateway_status_checked"] = current_status

    if current_status == "UNKNOWN":
        print(f"[WA_WARM_STATUS_UNKNOWN_SKIP] deal_id={row.get('deal_id')} message_id={message_id}")
        return True

    if current_status == "SERVER_ACK":
        row["status"] = "SERVER_ACK"
        row["ack_at"] = iso_now()
        add_note(crm, row, f"[WA_WARM_SERVER_ACK]<br>phone={row.get('phone')}<br>message_id={message_id}<br>status={current_status}", apply=apply)
        print(f"[WA_WARM_SERVER_ACK] deal_id={row.get('deal_id')} message_id={message_id}")
        return True

    if current_status in {"DELIVERED", "READ", "PLAYED"}:
        row["status"] = "DELIVERED"
        row["delivered_at"] = iso_now()
        add_note(crm, row, f"[WA_WARM_DELIVERED]<br>phone={row.get('phone')}<br>message_id={message_id}<br>status={current_status}", apply=apply)
        print(f"[WA_WARM_DELIVERY_OK] deal_id={row.get('deal_id')} message_id={message_id} status={current_status}")
        return True

    if current_status in {"NOT_FOUND", "INVALID"}:
        create_call_activity_once(crm, row, "Revisar telefone WhatsApp", apply=apply)
        add_note(crm, row, f"[WA_WARM_INVALID_OR_NOT_FOUND]<br>phone={row.get('phone')}<br>message_id={message_id}<br>status={current_status}", apply=apply)
        row["status"] = current_status
        row["fallback_done"] = True
        row["fallback_done_at"] = iso_now()
        print(f"[WA_WARM_INVALID_OR_NOT_FOUND] deal_id={row.get('deal_id')} message_id={message_id}")
        return True

    subject = "Contato warm - WhatsApp pendente"
    create_call_activity_once(crm, row, subject, apply=apply)
    person = crm.get_person_details(int(row.get("person_id") or 0)) if int(row.get("person_id") or 0) else {}
    email_addr = extract_email(person)
    email_ok = send_contextual_email(email_addr, row, apply=apply)
    add_note(
        crm,
        row,
        (
            "[WA_WARM_PENDING_FALLBACK]<br>"
            "WhatsApp ficou pendente; fallback criado por ligação/email.<br>"
            f"phone={row.get('phone')}<br>"
            f"message_id={message_id}<br>"
            f"email_fallback={'sim' if email_ok else 'nao'}"
        ),
        apply=apply,
    )
    row["status"] = "PENDING"
    row["fallback_done"] = True
    row["fallback_done_at"] = iso_now()
    print(f"[WA_WARM_PENDING_FALLBACK] deal_id={row.get('deal_id')} phone={row.get('phone')} message_id={message_id}")
    return True


def main() -> int:
    load_dotenv(ROOT / ".env")
    load_dotenv("/root/sdr-vps/.env")
    parser = argparse.ArgumentParser(description="Reconcile WhatsApp warm messages that stayed PENDING.")
    parser.add_argument("--apply", action="store_true", help="Create Pipedrive notes/activities and send fallback email.")
    parser.add_argument("--seed-gabriel", action="store_true", help="Insert/update Gabriel pending record before reconciliation.")
    args = parser.parse_args()

    state = load_state()
    changed = False
    if args.seed_gabriel:
        changed = add_seed_gabriel(state) or changed

    crm = PipedriveClient()
    for key, row in list(state.items()):
        try:
            changed = process_row(crm, key, row, apply=bool(args.apply)) or changed
        except Exception as exc:
            print(f"[WA_WARM_RECONCILE_ROW_ERROR] message_id={key} deal_id={row.get('deal_id')} error={exc}")
            row["last_error"] = str(exc)
            row["last_checked_at"] = iso_now()
            changed = True

    if changed and args.apply:
        save_state(state)
    elif changed:
        print("[WA_WARM_RECONCILE_DRY_RUN] state changes not persisted; use --apply")
    print(f"[WA_WARM_RECONCILE_DONE] total={len(state)} apply={1 if args.apply else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
