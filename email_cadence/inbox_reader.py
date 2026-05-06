import email
import imaplib
import json
import os
import re
import requests
from datetime import datetime, timedelta
from email.header import decode_header
from pathlib import Path
from core.sdr_state import STAGE_PRONTO_PROSPECCAO, mark_inbound, mark_warm, log_event

IMAP_HOST = os.getenv("OUTBOUND_IMAP_HOST", "")
IMAP_USER = os.getenv("OUTBOUND_IMAP_USER", "")
IMAP_PASS = os.getenv("OUTBOUND_IMAP_PASS", "")
TOKEN = os.getenv("PIPEDRIVE_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE = "https://api.pipedrive.com/v1"
QUEUE = Path("/root/sdr-vps/data/email_cadence_queue.json")
DEDUP = Path("/root/sdr-vps/data/email_inbox_reader_seen.json")
LOOKBACK_DAYS = max(1, int(os.getenv("OUTBOUND_IMAP_LOOKBACK_DAYS", "7") or "7"))

RESPONDIDO_LABEL_ID = 196
WARM_WHATSAPP_LABEL_ID = 226


def load_json(path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if data is not None else default
    except Exception:
        return default


def save_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def clean_sender(raw):
    m = re.search(r"<([^>]+)>", raw or "")
    return (m.group(1) if m else raw or "").strip().lower()


def decode_subject(s):
    if not s:
        return ""
    parts = decode_header(s)
    out = ""
    for txt, enc in parts:
        if isinstance(txt, bytes):
            out += txt.decode(enc or "utf-8", errors="ignore")
        else:
            out += txt
    return out


def api(method, path, **kw):
    if not TOKEN:
        print("[PIPEDRIVE_SKIP] token ausente")
        return None
    params = kw.pop("params", {})
    params["api_token"] = TOKEN
    r = requests.request(method, BASE + path, params=params, timeout=20, **kw)
    if r.status_code >= 400:
        print("[PIPEDRIVE_FAIL]", method, path, r.status_code, r.text[:200])
        return None
    return r.json() if r.text else {}


def queued_rows():
    rows = load_json(QUEUE, [])
    return rows if isinstance(rows, list) else []


def find_deal_by_email(addr):
    target = str(addr or "").strip().lower()
    if not target:
        return None, None
    for row in queued_rows():
        if str(row.get("email") or "").strip().lower() == target:
            return int(row["deal_id"]), str(row.get("email") or "").strip().lower()
    return None, None


def stop_cadence(addr, deal_id):
    if not QUEUE.exists():
        return
    q = queued_rows()
    changed = 0
    sender = str(addr or "").strip().lower()
    for x in q:
        if int(x.get("deal_id", 0)) == int(deal_id) or str(x.get("email") or "").strip().lower() == sender:
            if x.get("status") not in ("done", "stopped", "replied"):
                x["status"] = "replied"
                x["stop_reason"] = "email_replied"
                x["stopped_at"] = datetime.now().isoformat(timespec="seconds")
                changed += 1
    save_json(QUEUE, q)
    print("[CADENCE_STOPPED]", deal_id, sender, changed)


def normalize_label_items(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, str):
        return [item.strip() for item in raw.split(",") if item.strip()]
    return [raw]


def label_key(item):
    if isinstance(item, dict):
        return str(item.get("id") or item.get("label") or item.get("name") or item.get("value") or "").strip()
    return str(item or "").strip()


def merge_labels(deal_id, *label_ids):
    deal_resp = api("GET", f"/deals/{int(deal_id)}")
    deal = (deal_resp or {}).get("data") or {}
    current = normalize_label_items(deal.get("label"))
    keys = {label_key(item) for item in current if label_key(item)}
    for label_id in label_ids:
        if str(label_id) not in keys:
            current.append(int(label_id))
            keys.add(str(label_id))
    return api("PUT", f"/deals/{int(deal_id)}", json={"label": current})


def mark_crm(deal_id, sender, subject, message_id):
    note = (
        "<b>Resposta recebida por email</b><br>"
        f"<b>De:</b> {sender}<br>"
        f"<b>Assunto:</b> {subject}<br>"
        f"<b>Message-ID:</b> {message_id}<br>"
        "<b>Acao:</b> cadencia parada automaticamente."
    )
    api("POST", "/notes", json={"deal_id": int(deal_id), "content": note})
    merge_labels(deal_id, RESPONDIDO_LABEL_ID, WARM_WHATSAPP_LABEL_ID)
    api("PUT", f"/deals/{int(deal_id)}", json={"stage_id": STAGE_PRONTO_PROSPECCAO})
    mark_warm(deal_id, source="email_reply", score_event="email_reply")
    mark_inbound(deal_id, "", f"{sender} | {subject}", intent="replied", origin="email_reply")
    log_event("EMAIL_REPLY_STOP", deal_id=deal_id, email=sender)
    print("[CRM_RESPONDIDO]", deal_id)


def seen_key(msg, sender):
    message_id = str(msg.get("Message-ID") or msg.get("Message-Id") or "").strip()
    if message_id:
        return message_id
    date = str(msg.get("Date") or "").strip()
    subject = decode_subject(msg.get("Subject", ""))
    return f"{sender}|{date}|{subject}"


def run():
    if not all([IMAP_HOST, IMAP_USER, IMAP_PASS]):
        print("[IMAP_SKIP] OUTBOUND_IMAP_HOST/USER/PASS ausentes")
        return

    seen = load_json(DEDUP, {})
    if not isinstance(seen, dict):
        seen = {}
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    since = cutoff.strftime("%d-%b-%Y")

    mail = imaplib.IMAP4_SSL(IMAP_HOST)
    mail.login(IMAP_USER, IMAP_PASS)
    mail.select("INBOX")
    typ, data = mail.search(None, f'(SINCE "{since}")')
    if typ != "OK":
        print("[IMAP_SEARCH_FAIL]", typ)
        mail.logout()
        return

    processed = 0
    for num in data[0].split():
        typ, msg_data = mail.fetch(num, "(RFC822)")
        if typ != "OK" or not msg_data or not msg_data[0]:
            continue
        msg = email.message_from_bytes(msg_data[0][1])
        sender = clean_sender(msg.get("From", ""))
        subject = decode_subject(msg.get("Subject", ""))
        key = seen_key(msg, sender)
        if key in seen:
            continue

        if "postmaster" in sender or "mailer-daemon" in sender:
            print("[BOUNCE_IGNORED]", sender, subject)
            seen[key] = {"ignored": "bounce", "at": datetime.now().isoformat(timespec="seconds")}
            continue

        deal_id, queued_email = find_deal_by_email(sender)
        print("[EMAIL_REPLY]", sender, subject, "deal=", deal_id)

        if deal_id:
            mark_crm(deal_id, sender, subject, key)
            stop_cadence(queued_email or sender, deal_id)
            processed += 1
            seen[key] = {"deal_id": deal_id, "email": queued_email or sender, "at": datetime.now().isoformat(timespec="seconds")}
        else:
            seen[key] = {"ignored": "not_in_queue", "email": sender, "at": datetime.now().isoformat(timespec="seconds")}

    # Mantem o arquivo pequeno sem perder dedupe recente.
    if len(seen) > 5000:
        seen = dict(list(seen.items())[-5000:])
    save_json(DEDUP, seen)
    mail.logout()
    print("[INBOX_READER_DONE]", processed)


if __name__ == "__main__":
    run()
