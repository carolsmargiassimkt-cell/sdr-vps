from __future__ import annotations

import json
import os
import smtplib
from crm.sdr_field_updater import update_sdr_fields
import ssl
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import requests

from core.crm_hygiene import is_generic_email
from core.sdr_state import LABEL_RESPONDIDO, log_event, mark_email_cadence


BASE_DIR = Path(__file__).resolve().parents[1]
DATA = BASE_DIR / "data" / "email_cadence_queue.json"
EVENTS = BASE_DIR / "data" / "email_cadence_events.json"
DATA.parent.mkdir(parents=True, exist_ok=True)

PD = os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN") or ""
FROM = os.getenv("SMTP_FROM_EMAIL", "")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465") or 465)
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
TRACKING_BASE_URL = os.getenv("TRACKING_BASE_URL", "http://191.252.184.140:8001").rstrip("/")
FORM_URL = os.getenv("PREFORM_URL", "https://docs.google.com/forms/d/1KWo-Z7uKflvpR0Ff9yxFJhyV-iFtm0v4xH3QKC9FRmo/viewform?usp=header")
CALENDLY_URL = os.getenv("CALENDLY_URL", "https://calendly.com/ana-manddigital/30min")

STEPS = [0, 2, 4, 7, 10, 14]
STOP_STATUSES = {"clicked_warm", "replied", "won", "lost", "opt_out", "wrong_contact", "done", "stopped", "warm", "shared_email_blocked", "shared_phone_blocked"}
DAILY_LIMIT = int(os.getenv("EMAIL_DAILY_LIMIT", "50") or "50")
MIN_DELAY_SECONDS = float(os.getenv("EMAIL_MIN_DELAY_SECONDS", "45") or "45")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        return payload if payload is not None else default
    except Exception:
        return default


def save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_phone(value: Any) -> str:
    import re

    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits


def row_email(row: dict[str, Any]) -> str:
    return str(row.get("email") or "").strip().lower()


def row_phone(row: dict[str, Any]) -> str:
    return normalize_phone(row.get("phone"))


def idempotency_key(deal_id, step):
    return f"{int(deal_id)}:{int(step)}"


def load() -> list[dict[str, Any]]:
    rows = load_json(DATA, [])
    return rows if isinstance(rows, list) else []


def save(rows) -> None:
    save_json(DATA, rows if isinstance(rows, list) else [])


def save_queue_confirmed(rows, row, step) -> None:
    try:
        save(rows)
    except Exception as exc:
        print("[EMAIL_QUEUE_SAVE_FAIL]", row.get("deal_id"), step, type(exc).__name__, str(exc)[:200])
        log_event("EMAIL_QUEUE_SAVE_FAIL", deal_id=row.get("deal_id"), step=step, error=str(exc)[:200])
        raise
    print("[EMAIL_QUEUE_SAVE_CONFIRMED]", row.get("deal_id"), step)


def load_events() -> list[dict[str, Any]]:
    rows = load_json(EVENTS, [])
    return rows if isinstance(rows, list) else []


def save_events(rows) -> None:
    save_json(EVENTS, rows if isinstance(rows, list) else [])


def event_exists(deal_id, step) -> bool:
    key = idempotency_key(deal_id, step)
    return any(str(ev.get("idempotency_key") or "") == key for ev in load_events())


def record_sent_event(row, step, subject, sent_at) -> bool:
    key = idempotency_key(row.get("deal_id"), step)
    events = load_events()
    if any(str(ev.get("idempotency_key") or "") == key for ev in events):
        return False
    events.append({
        "deal_id": int(row.get("deal_id")),
        "email": row_email(row),
        "phone": row_phone(row),
        "step": int(step),
        "subject": str(subject or ""),
        "sent_at": sent_at,
        "status": "sent",
        "idempotency_key": key,
    })
    save_events(events)
    return True


def pd_request(method, path, json_body=None):
    if not PD:
        return None
    response = requests.request(
        method,
        f"https://api.pipedrive.com/v1/{path.lstrip('/')}",
        params={"api_token": PD},
        json=json_body,
        timeout=30,
    )
    if response.status_code >= 400:
        print("[PD_FAIL]", method, path, response.status_code, response.text[:200])
        return None
    return response.json() if response.text else {}


def deal_label_tokens(raw) -> set[str]:
    if raw is None:
        return set()
    if isinstance(raw, list):
        return {str(item.get("id") if isinstance(item, dict) else item).strip() for item in raw if str(item or "").strip()}
    return {item.strip() for item in str(raw or "").split(",") if item.strip()}


def crm_stop_reason(deal_id) -> str:
    payload = pd_request("GET", f"/deals/{int(deal_id)}")
    deal = (payload or {}).get("data") or {}
    status = str(deal.get("status") or "").strip().lower()
    if status in {"won", "lost"}:
        return status
    labels = deal_label_tokens(deal.get("label"))
    if str(LABEL_RESPONDIDO) in labels:
        return "replied"
    return ""


def queue_conflict(rows, deal_id, email, phone) -> str:
    target_email = str(email or "").strip().lower()
    target_phone = normalize_phone(phone)
    for row in rows:
        if int(row.get("deal_id") or 0) == int(deal_id):
            return "already_queued"
        status = str(row.get("status") or "").strip()
        if status in STOP_STATUSES:
            continue
        if target_email and row_email(row) == target_email and int(row.get("deal_id") or 0) != int(deal_id):
            return "shared_email_blocked"
        if target_phone and row_phone(row) == target_phone and int(row.get("deal_id") or 0) != int(deal_id):
            return "shared_phone_blocked"
    return ""


def enqueue(deal_id, email, nome="", empresa="", phone="", **metadata):
    clean_email = str(email or "").strip().lower()
    clean_phone = normalize_phone(phone)
    if is_generic_email(clean_email):
        log_event("EMAIL_QUEUE_SKIP", deal_id=deal_id, email=clean_email, reason="generic_or_invalid_email")
        return {"ok": True, "skip": "generic_or_invalid_email"}
    rows = load()
    conflict = queue_conflict(rows, deal_id, clean_email, clean_phone)
    if conflict:
        if conflict.startswith("shared_"):
            rows.append({
                "deal_id": int(deal_id),
                "email": clean_email,
                "phone": clean_phone,
                "nome": nome,
                "empresa": empresa,
                "step": 1,
                "status": conflict,
                "next_send": None,
                "created_at": now(),
                "source": str(metadata.get("source") or "outbound"),
            })
            save(rows)
        log_event("EMAIL_QUEUE_SKIP", deal_id=deal_id, email=clean_email, phone=clean_phone, reason=conflict)
        return {"ok": True, "skip": conflict}
    row = {
        "deal_id": int(deal_id),
        "email": clean_email,
        "phone": clean_phone,
        "nome": nome,
        "empresa": empresa,
        "step": 1,
        "status": "pending",
        "next_send": now(),
        "created_at": now(),
        "last_sent_step_email": 0,
        "source": str(metadata.get("source") or "outbound"),
    }
    rows.append(row)
    save(rows)
    mark_email_cadence(deal_id, email=clean_email, phone=clean_phone, active=True, origin="outbound")
    log_event("EMAIL_QUEUE_ADD", deal_id=deal_id, email=clean_email, phone=clean_phone)
    return {"ok": True, "queued": int(deal_id)}


def cta_html(row, step):
    deal_id = row.get("deal_id") or row.get("id")
    forms = f"{TRACKING_BASE_URL}/t/{deal_id}/{step}?r={FORM_URL}"
    calendly = f"{TRACKING_BASE_URL}/t/{deal_id}/{step}?r={CALENDLY_URL}"
    return (
        "<br><br>Se fizer sentido, voce pode:<br><br>"
        f"<a href='{forms}' style='color:#2563eb;text-decoration:underline;font-weight:600;'>Responder em 1 minuto</a><br><br>"
        f"<a href='{calendly}' style='color:#16a34a;text-decoration:underline;font-weight:600;'>Ou agendar direto comigo</a>"
    )


def open_pixel(row, step):
    return f"<img src='{TRACKING_BASE_URL}/o/{row.get('deal_id')}/{step}.gif' width='1' height='1' style='display:none' alt='' />"


def template(step, row):
    nome = (row.get("nome") or "").split(" ")[0].title() or "tudo bem"
    empresa = row.get("empresa") or "a empresa"
    cta = cta_html(row, step)
    pixel = open_pixel(row, step)
    textos = {
        1: ("duvida rapida sobre campanhas", f"Oi {nome}, tudo bem?<br><br>Queria te fazer uma pergunta direta: hoje voces usam campanhas promocionais para gerar venda ou o crescimento vem mais do fluxo natural?{cta}<br><br>Abs,<br>Carol{pixel}"),
        2: ("o que tenho visto no mercado", f"Oi {nome},<br><br>Vejo um padrao em empresas como {empresa}: campanhas geram movimento, mas nem sempre capturam dados uteis depois. Com roleta, raspadinha ou premio instantaneo da para medir melhor o interesse.{cta}<br><br>Abs,<br>Carol{pixel}"),
        3: ("campanha como gatilho", f"Oi {nome},<br><br>Voces ja tem alguma acao para aproveitar datas fortes e aumentar participacao do publico? A Mand estrutura campanhas interativas com captura de dados proprios.{cta}<br><br>Abs,<br>Carol{pixel}"),
        4: ("campanha que vira dado", f"Oi {nome},<br><br>Um ponto comum: muita campanha gera exposicao, mas pouca inteligencia para o proximo passo. Quando o cliente participa, da para entender interesse, canal e intencao.{cta}<br><br>Abs,<br>Carol{pixel}"),
        5: ("antes da proxima campanha", f"Oi {nome},<br><br>Quem estrutura campanha antes da data comercial captura mais valor durante o pico: movimento, base propria, dados e recorrencia.{cta}<br><br>Abs,<br>Carol{pixel}"),
        6: ("faz sentido ou encerro por aqui?", f"Oi {nome},<br><br>Prometo ser minha ultima mensagem. Campanhas interativas orientadas a dados fazem sentido para {empresa} agora ou nao e prioridade?{cta}<br><br>Abs,<br>Carol{pixel}"),
    }
    return textos.get(int(step), textos[1])


def send_smtp(row, subject, body):
    to = row_email(row)
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, FROM]):
        print("[SMTP_NOT_CONFIGURED]", to, subject)
        return {"ok": False, "error": "smtp_not_configured"}
    msg = EmailMessage()
    msg["From"] = FROM
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body.replace("<br>", "\n"))
    msg.add_alternative(body, subtype="html")
    ctx = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    except Exception as exc:
        print("[SMTP_SEND_FAIL]", to, type(exc).__name__, str(exc)[:200])
        return {"ok": False, "error": "smtp_send_failed"}

    print("[SMTP_SEND_CONFIRMED]", to, row.get("deal_id"), row.get("step"))
    return {"ok": True, "dry_run": False}


def advance_after_send(row, step, sent_at):
    row["last_sent_step_email"] = int(step)
    row["last_sent_at"] = sent_at
    row.pop("hold_reason", None)
    if step >= 6:
        row["status"] = "done"
        row["next_send"] = None
    else:
        row["step"] = step + 1
        row["status"] = "pending"
        row["next_send"] = (datetime.now() + timedelta(days=STEPS[step])).isoformat(timespec="seconds")


def tick():
    rows = load()
    changed = False
    sent_today = 0
    current = datetime.now()
    for row in rows:
        status = str(row.get("status") or "")
        if status in STOP_STATUSES:
            continue
        stop_reason = crm_stop_reason(row.get("deal_id"))
        if stop_reason:
            row["status"] = stop_reason
            row["stopped_at"] = now()
            row["stop_reason"] = f"crm_{stop_reason}"
            changed = True
            log_event("EMAIL_REPLY_STOP", deal_id=row.get("deal_id"), reason=stop_reason)
            continue
        if status != "pending":
            continue
        if sent_today >= DAILY_LIMIT:
            break
        if not row.get("next_send") or datetime.fromisoformat(row["next_send"]) > current:
            continue
        step = max(1, min(6, int(row.get("step") or 1)))
        if int(row.get("last_sent_step_email") or 0) >= step or event_exists(row.get("deal_id"), step):
            advance_after_send(row, step, now())
            print("[EMAIL_QUEUE_ADVANCED]", row.get("deal_id"), step, "idempotent")
            changed = True
            continue
        subject, body = template(step, row)
        result = send_smtp(row, subject, body)
        if not (isinstance(result, dict) and result.get("ok")):
            continue
        sent_at = now()
        advance_after_send(row, step, sent_at)
        print("[EMAIL_QUEUE_ADVANCED]", row.get("deal_id"), step)
        save_queue_confirmed(rows, row, step)
        changed = True
        if not result.get("dry_run"):
            if record_sent_event(row, step, subject, sent_at):
                log_event("EMAIL_SENT", deal_id=row.get("deal_id"), email=row.get("email"), step=step, subject=subject)
            try:
                update_sdr_fields(row.get("deal_id"), {
                    "event_id": f"email_sent:{row.get('deal_id')}:{step}",
                    "type": "email_sent",
                    "channel": "email",
                    "source": "email_cadence",
                    "step": step,
                    "cadence_step": step,
                    "automation_status": "email_sent",
                    "status_sdr": "em_cadencia_email",
                    "increment_attempt": True,
                })
            except Exception as exc:
                print("[SDR_FIELDS_EMAIL_SENT_FAIL]", row.get("deal_id"), exc)
            sent_today += 1
            time.sleep(MIN_DELAY_SECONDS)
    if changed:
        save(rows)
    print("[CADENCE_TICK_OK]")


if __name__ == "__main__":
    tick()
