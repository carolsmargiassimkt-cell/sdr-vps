from __future__ import annotations

import os
import re

import requests
from dotenv import load_dotenv
from fastapi import APIRouter, Header, HTTPException, Request

from core.sdr_state import (
    LABEL_LEAD_TRAFEGO,
    PIPELINE_ID,
    STAGE_PRONTO_PROSPECCAO,
    log_event,
    mark_warm,
)
from core.crm_hygiene import is_generic_email, person_org_id


router = APIRouter()
load_dotenv("/root/sdr-vps/.env", override=True)

TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN", "")
PD = os.getenv("PIPEDRIVE_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN") or ""
BASE = "https://api.pipedrive.com/v1"
INTERNAL_DOMAIN = "manddigital.com.br"


def only_digits(value):
    return re.sub(r"\D+", "", str(value or ""))


def normalize_phone(value):
    digits = only_digits(value)
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits


def first_value(body, *names):
    for name in names:
        value = body.get(name)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def clean_crm_name(raw, email=""):
    txt = re.sub(r"<br\s*/?>", "\n", str(raw or ""), flags=re.I)
    txt = re.sub(r"<[^>]+>", "", txt).strip()
    for line in txt.splitlines():
        line = line.strip()
        if line and not line.lower().startswith(("mensagem:", "data:", "horario:", "url da pagina", "agente")):
            return line[:80]
    if email and "@" in email:
        return email.split("@")[0].replace(".", " ").replace("_", " ").title()
    return "Lead sem nome"


def pd(method, path, **kwargs):
    if not PD:
        print("[PD_SKIP] token ausente")
        return None
    params = kwargs.pop("params", {})
    params["api_token"] = PD
    r = requests.request(method, BASE + path, params=params, timeout=20, **kwargs)
    print("[PD]", method, path, r.status_code, r.text[:200])
    if r.status_code >= 400:
        return None
    return r.json().get("data") if r.text else {}


def search_first(entity, term):
    clean = str(term or "").strip()
    if not clean:
        return {}
    data = pd("GET", f"/{entity}/search", params={"term": clean, "limit": 10}) or {}
    for item in data.get("items") or []:
        found = dict(item.get("item") or {})
        if found:
            return found
    return {}


def find_person(email, phone):
    for term in [email, normalize_phone(phone), only_digits(phone)]:
        person = search_first("persons", term)
        if person:
            return person
    return {}


def find_org(name):
    return search_first("organizations", name)


def find_org_by_cnpj(cnpj):
    clean = only_digits(cnpj)
    if not clean:
        return {}
    return search_first("organizations", clean)


def label_items(raw):
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


def merge_labels(raw, *label_ids):
    current = label_items(raw)
    keys = {label_key(item) for item in current if label_key(item)}
    for label_id in label_ids:
        if str(label_id) not in keys:
            current.append(int(label_id))
            keys.add(str(label_id))
    return current


def find_open_deal(person_id, org_id):
    if person_id:
        deals = pd("GET", f"/persons/{int(person_id)}/deals", params={"status": "open"}) or []
        for deal in deals:
            if int(deal.get("pipeline_id") or PIPELINE_ID) == PIPELINE_ID:
                return dict(deal)
    if org_id:
        deals = pd("GET", "/deals", params={"status": "open", "pipeline_id": PIPELINE_ID, "limit": 100}) or []
        for deal in deals:
            current_org = deal.get("org_id") or {}
            current_org_id = current_org.get("value") if isinstance(current_org, dict) else current_org
            if int(current_org_id or 0) == int(org_id):
                return dict(deal)
    return {}


def add_audit_note(person_id=0, deal_id=0, content=""):
    payload = {"content": content}
    if deal_id:
        payload["deal_id"] = int(deal_id)
    if person_id:
        payload["person_id"] = int(person_id)
    pd("POST", "/notes", json=payload)


def safe_update_person(pid, person_payload, current_person, target_org_id):
    existing_org_id = person_org_id(current_person)
    if existing_org_id and target_org_id and existing_org_id != int(target_org_id):
        return False, existing_org_id
    payload = dict(person_payload)
    if target_org_id:
        payload["org_id"] = int(target_org_id)
    pd("PUT", f"/persons/{int(pid)}", json=payload)
    return True, existing_org_id or target_org_id


@router.post("/webhooks/forms-lead")
async def forms_lead(req: Request, authorization: str = Header(None)):
    token = (os.getenv("WEBHOOK_AUTH_TOKEN") or TOKEN or "").strip()
    if not token or (authorization or "").strip() != f"Bearer {token}":
        print("[FORMS_AUTH_FAIL]", "auth=", authorization, "token_loaded=", bool(token))
        raise HTTPException(status_code=401, detail="unauthorized")

    body = await req.json()
    email = first_value(body, "email", "e-mail", "mail", "person_email").lower()
    whatsapp = first_value(body, "whatsapp", "telefone", "phone", "celular")
    nome = clean_crm_name(first_value(body, "nome", "name", "lead_name"), email=email)
    empresa = first_value(body, "empresa", "company", "organizacao", "organization")
    cnpj = first_value(body, "cnpj", "cnpj_empresa", "documento")
    segmento = first_value(body, "segmento", "segment")
    unidades = first_value(body, "unidades", "units")
    objetivo = first_value(body, "objetivo", "campanhas", "mensagem", "message")

    if email.endswith("@" + INTERNAL_DOMAIN):
        log_event("FORM_REJECTED", email=email, reason="internal_domain")
        return {"ok": True, "created": False, "skip": "internal_domain"}
    if email and is_generic_email(email):
        log_event("FORM_REJECTED", email=email, reason="generic_or_shared_email")
        return {"ok": True, "created": False, "skip": "generic_or_shared_email"}
    if not email and not whatsapp:
        log_event("FORM_REJECTED", reason="missing_contact")
        return {"ok": True, "created": False, "skip": "missing_contact"}

    log_event("FORM_ACCEPTED", email=email, phone=whatsapp, source="form")

    org = find_org_by_cnpj(cnpj) if cnpj else {}
    if not org:
        org = find_org(empresa) if empresa else {}
    if empresa and not org:
        org_payload = {"name": empresa}
        field = os.getenv("PIPEDRIVE_ORG_CNPJ_FIELD") or os.getenv("ORG_CNPJ_FIELD_KEY")
        if field and cnpj:
            org_payload[field] = only_digits(cnpj)
        org = pd("POST", "/organizations", json=org_payload) or {}
    org_id = int(org.get("id") or 0)

    person = find_person(email, whatsapp)
    person_payload = {
        "name": nome,
        "email": [{"value": email, "primary": True, "label": "work"}] if email else [],
        "phone": [{"value": whatsapp, "primary": True, "label": "whatsapp"}] if whatsapp else [],
    }
    if org_id:
        person_payload["org_id"] = org_id
    if person:
        pid = int(person.get("id") or 0)
        ok_update, existing_org_id = safe_update_person(pid, person_payload, person, org_id)
        if not ok_update:
            deal = find_open_deal(pid, existing_org_id) or {}
            deal_id = int(deal.get("id") or 0)
            note = (
                "<b>Auditoria CRM - formulario bloqueado</b><br>"
                f"Pessoa existente {pid} pertence a org {existing_org_id}, mas o formulario apontou org {org_id}.<br>"
                f"Email: {email}<br>WhatsApp: {whatsapp}<br>Empresa informada: {empresa}<br>CNPJ: {cnpj}<br>"
                "Acao: pending_review/shared_email_blocked; nenhuma automacao foi disparada."
            )
            add_audit_note(person_id=pid, deal_id=deal_id, content=note)
            log_event("FORM_REJECTED", email=email, phone=whatsapp, reason="person_org_conflict", person_id=pid, org_id=org_id, existing_org_id=existing_org_id)
            return {
                "ok": True,
                "created": False,
                "skip": "pending_review_person_org_conflict",
                "person_id": pid,
                "existing_org_id": existing_org_id,
                "incoming_org_id": org_id,
            }
    else:
        person = pd("POST", "/persons", json=person_payload) or {}
        pid = int(person.get("id") or 0)
    if not pid:
        return {"ok": False, "error": "person_fail"}

    deal = find_open_deal(pid, org_id)
    if deal:
        deal_id = int(deal.get("id") or 0)
        labels = merge_labels(deal.get("label"), LABEL_LEAD_TRAFEGO)
        pd("PUT", f"/deals/{deal_id}", json={
            "person_id": pid,
            "org_id": org_id or None,
            "pipeline_id": PIPELINE_ID,
            "stage_id": STAGE_PRONTO_PROSPECCAO,
            "label": labels,
        })
    else:
        deal = pd("POST", "/deals", json={
            "title": f"TRAFEGO - {nome}",
            "person_id": pid,
            "org_id": org_id or None,
            "pipeline_id": PIPELINE_ID,
            "stage_id": STAGE_PRONTO_PROSPECCAO,
            "label": [LABEL_LEAD_TRAFEGO],
        }) or {}
        deal_id = int(deal.get("id") or 0)
    if not deal_id:
        return {"ok": False, "error": "deal_fail", "person_id": pid}

    note = f"""
<b>Formulario inbound/trafego recebido</b><br>
<b>Nome:</b> {nome}<br>
<b>Email:</b> {email}<br>
<b>WhatsApp:</b> {whatsapp}<br>
<b>Empresa:</b> {empresa}<br>
<b>Segmento:</b> {segmento}<br>
<b>Unidades:</b> {unidades}<br>
<b>Objetivo:</b> {objetivo}<br>
<b>Acao:</b> LEAD_TRAFEGO aplicado e deal movido para Pronto para Prospeccao.
"""
    pd("POST", "/notes", json={"deal_id": deal_id, "content": note})
    mark_warm(deal_id, phone=whatsapp, source="lead_trafego", score_event="form")
    log_event("CRM_LABEL_UPDATE", deal_id=deal_id, label=LABEL_LEAD_TRAFEGO, source="form")
    log_event("CRM_STAGE_UPDATE", deal_id=deal_id, stage_id=STAGE_PRONTO_PROSPECCAO, source="form")
    return {"ok": True, "deal_id": deal_id, "person_id": pid, "stage_id": STAGE_PRONTO_PROSPECCAO, "label": LABEL_LEAD_TRAFEGO}
