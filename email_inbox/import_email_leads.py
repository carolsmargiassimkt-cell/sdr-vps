from __future__ import annotations

import email
import imaplib
import os
import re
import time
from pathlib import Path
from email.header import decode_header

import requests
from dotenv import load_dotenv

from core.sdr_state import (
    LABEL_LEAD_TRAFEGO,
    PIPELINE_ID,
    STAGE_PRONTO_PROSPECCAO,
    log_event,
    mark_warm,
)


load_dotenv("/root/sdr-vps/.env")

IMAP_HOST = os.getenv("LEADS_IMAP_HOST", "mail.manddigital.com.br")
IMAP_USER = os.getenv("LEADS_IMAP_USER")
IMAP_PASS = os.getenv("LEADS_IMAP_PASS")
PD = os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN") or ""
API = "https://api.pipedrive.com/v1"
PROCESSED = Path("/root/sdr-vps/data/email_inbox_processed.txt")
INTERNAL_DOMAIN = "manddigital.com.br"

NOISE_SUBJECTS = (
    "nota fiscal", "pedido", "boleto", "nf-e", "danfe", "res:", "fw:", "enc:", "re:", "html viewer"
)
NOISE_ATTACHMENTS = ("image001.jpg", "image002.jpg", "image001.png", "image002.png")


def hdr(value):
    if not value:
        return ""
    out = ""
    for part, enc in decode_header(value):
        out += part.decode(enc or "utf-8", "ignore") if isinstance(part, bytes) else part
    return out.strip()


def get_body(msg):
    parts = msg.walk() if msg.is_multipart() else [msg]
    for part in parts:
        if part.get_content_type() in ("text/plain", "text/html"):
            raw = part.get_payload(decode=True) or b""
            return raw.decode(part.get_content_charset() or "utf-8", "ignore")
    return ""


def clean_html(text):
    text = re.sub(r"<br\s*/?>", "\n", str(text or ""), flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def is_noise_email(subject, text, msg):
    subj = str(subject or "").strip().lower()
    body = str(text or "")
    if any(token in subj for token in NOISE_SUBJECTS):
        return "noise_subject"
    filenames = " ".join(str(part.get_filename() or "").lower() for part in msg.walk())
    if any(name in filenames for name in NOISE_ATTACHMENTS):
        return "inline_image_thread"
    if body.count("De:") + body.count("From:") >= 2 or body.count("Cc:") >= 2:
        return "email_thread"
    return ""


def is_form_source(sender, subject):
    s = str(sender or "").lower()
    subj = str(subject or "").lower()
    return (
        ("leads@leadster.com.br" in s and "novo lead gerado" in subj)
        or ("comercial@manddigital.com.br" in s and "formulario lp" in subj)
        or ("comercial@manddigital.com.br" in s and "formul" in subj)
    )


def field(text, names):
    for name in names:
        match = re.search(
            rf"{name}\s*[:\-]\s*(.+?)(?=\s+(?:Nome|Email|E-mail|Telefone|WhatsApp|Empresa|Mensagem|Data|Horario)\s*[:\-]|$)",
            text,
            re.I,
        )
        if match:
            return clean_html(match.group(1))[:120]
    return ""


def extract(text, sender):
    txt = clean_html(text.replace("<br>", "\n"))
    emails = [
        item.lower()
        for item in re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", txt + " " + sender)
        if INTERNAL_DOMAIN not in item.lower() and "leadster" not in item.lower()
    ]
    phones = re.findall(r"(?:\+?55)?\s?\(?\d{2}\)?\s?\d{4,5}[-\s]?\d{4}", txt)
    nome = field(txt, ["nome", "name"])
    empresa = field(txt, ["empresa", "company", "loja", "organizacao"])
    msg = field(txt, ["mensagem", "message", "objetivo"])
    if not nome and emails:
        nome = emails[0].split("@")[0].replace(".", " ").replace("_", " ").title()
    return {
        "nome": nome or "Lead LP",
        "empresa": empresa[:80],
        "email": emails[0] if emails else "",
        "phone": phones[0] if phones else "",
        "mensagem": msg,
        "texto": txt[:2500],
    }


def pd(method, path, body=None, params=None):
    params = params or {}
    params["api_token"] = PD
    r = requests.request(method, API + path, params=params, json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def search_person(email_addr, phone):
    for term in [email_addr, phone]:
        if not term:
            continue
        data = pd("GET", "/persons/search", params={"term":term, "exact_match":True}).get("data", {}).get("items", [])
        if data:
            return data[0]["item"]
    return None


def get_or_create_org(name):
    if not name:
        return None
    data = pd("GET", "/organizations/search", params={"term": name, "exact_match": True}).get("data", {}).get("items", [])
    if data:
        return data[0]["item"]["id"]
    return pd("POST", "/organizations", {"name": name})["data"]["id"]


def find_open_deal(person_id, org_id):
    if person_id:
        deals = pd("GET", f"/persons/{int(person_id)}/deals", params={"status": "open"}).get("data") or []
        for deal in deals:
            if int(deal.get("pipeline_id") or PIPELINE_ID) == PIPELINE_ID:
                return deal
    return None


def merge_label(raw, label_id):
    current = []
    if isinstance(raw, list):
        current = list(raw)
    elif raw:
        current = [item.strip() for item in str(raw).split(",") if item.strip()]
    keys = {str(item.get("id") if isinstance(item, dict) else item).strip() for item in current}
    if str(label_id) not in keys:
        current.append(int(label_id))
    return current


def main():
    print("[EMAIL_IMPORT_START]")
    processed = set(PROCESSED.read_text().splitlines()) if PROCESSED.exists() else set()
    imap = imaplib.IMAP4_SSL(IMAP_HOST, 993)
    imap.login(IMAP_USER, IMAP_PASS)
    imap.select("INBOX")
    _, data = imap.search(None, "ALL")

    for mid in data[0].split()[-80:]:
        mid_s = mid.decode()
        if mid_s in processed:
            continue
        _, raw = imap.fetch(mid, "(RFC822)")
        msg = email.message_from_bytes(raw[0][1])
        sender = hdr(msg.get("From"))
        subject = hdr(msg.get("Subject"))
        text = get_body(msg)

        reason = is_noise_email(subject, text, msg)
        if reason:
            log_event("FORM_REJECTED", source="email_inbox", reason=reason, subject=subject)
            processed.add(mid_s)
            continue
        if not is_form_source(sender, subject):
            processed.add(mid_s)
            continue

        lead = extract(text, sender)
        if not lead["email"] and not lead["phone"]:
            log_event("FORM_REJECTED", source="email_inbox", reason="missing_contact")
            processed.add(mid_s)
            continue
        if lead["email"].endswith("@" + INTERNAL_DOMAIN):
            log_event("FORM_REJECTED", source="email_inbox", reason="internal_domain", email=lead["email"])
            processed.add(mid_s)
            continue

        existing = search_person(lead["email"], lead["phone"])
        org_id = get_or_create_org(lead["empresa"])
        if existing:
            person_id = existing["id"]
            deal = find_open_deal(person_id, org_id)
        else:
            person_id = pd("POST", "/persons", {
                "name": lead["nome"],
                "org_id": org_id,
                "email": [{"value": lead["email"], "primary": True}] if lead["email"] else [],
                "phone": [{"value": lead["phone"], "primary": True}] if lead["phone"] else [],
            })["data"]["id"]
            deal = None

        if deal:
            deal_id = int(deal["id"])
            pd("PUT", f"/deals/{deal_id}", {
                "stage_id": STAGE_PRONTO_PROSPECCAO,
                "label": merge_label(deal.get("label"), LABEL_LEAD_TRAFEGO),
            })
        else:
            title = f"Lead LP - {lead['empresa'] or lead['nome']}".strip()
            deal = pd("POST", "/deals", {
                "title": title,
                "person_id": person_id,
                "org_id": org_id,
                "pipeline_id": PIPELINE_ID,
                "stage_id": STAGE_PRONTO_PROSPECCAO,
                "label": [LABEL_LEAD_TRAFEGO],
            })["data"]
            deal_id = int(deal["id"])

        pd("POST", "/notes", {
            "deal_id": deal_id,
            "person_id": person_id,
            "org_id": org_id,
            "content": f"Origem: Leadster/Formulario LP<br>Tag: LEAD_TRAFEGO<br>Assunto: {subject}<br><br>{lead['texto']}",
        })
        mark_warm(deal_id, phone=lead["phone"], source="lead_trafego", score_event="form")
        log_event("FORM_ACCEPTED", source="email_inbox", deal_id=deal_id, email=lead["email"], phone=lead["phone"])
        processed.add(mid_s)
        time.sleep(0.2)

    PROCESSED.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED.write_text("\n".join(sorted(processed, key=lambda value: int(value) if str(value).isdigit() else 0)))
    imap.logout()


if __name__ == "__main__":
    main()
