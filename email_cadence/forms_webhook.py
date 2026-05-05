from fastapi import APIRouter, Request, Header, HTTPException
import os
import re
import requests

router = APIRouter()

TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN")
PD = os.getenv("PIPEDRIVE_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE = "https://api.pipedrive.com/v1"

PIPELINE_ID = 7
STAGE_PRONTO_PROSPECCAO = 63
LEAD_TRAFEGO_LABEL_ID = 193


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


def only_digits(value):
    return re.sub(r"\D+", "", str(value or ""))


def normalize_phone(value):
    digits = only_digits(value)
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits


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


@router.post("/webhooks/forms-lead")
async def forms_lead(req: Request, authorization: str = Header(None)):
    if not TOKEN or authorization != f"Bearer {TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")

    body = await req.json()

    nome = (body.get("nome") or body.get("name") or "Lead Forms").strip()
    email = (body.get("email") or "").strip().lower()
    whatsapp = (body.get("whatsapp") or body.get("telefone") or body.get("phone") or "").strip()
    empresa = (body.get("empresa") or body.get("company") or "").strip()
    segmento = body.get("segmento") or ""
    unidades = body.get("unidades") or ""
    campanhas = body.get("campanhas") or ""
    objetivo = body.get("objetivo") or ""

    print("[FORMS_RECEBIDO]", nome, email, whatsapp)

    org = find_org(empresa) if empresa else {}
    if empresa and not org:
        org = pd("POST", "/organizations", json={"name": empresa}) or {}
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
        pd("PUT", f"/persons/{pid}", json=person_payload)
    else:
        person = pd("POST", "/persons", json=person_payload) or {}
        pid = int(person.get("id") or 0)

    if not pid:
        return {"ok": False, "error": "person_fail"}

    deal = find_open_deal(pid, org_id)
    if deal:
        deal_id = int(deal.get("id") or 0)
        labels = merge_labels(deal.get("label"), LEAD_TRAFEGO_LABEL_ID)
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
            "label": [LEAD_TRAFEGO_LABEL_ID],
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
<b>Campanhas:</b> {campanhas}<br>
<b>Objetivo:</b> {objetivo}<br>
<b>Acao:</b> LEAD_TRAFEGO aplicado e deal movido para Pronto para Prospeccao.
"""
    pd("POST", "/notes", json={"deal_id": deal_id, "content": note})

    return {
        "ok": True,
        "deal_id": deal_id,
        "person_id": pid,
        "org_id": org_id,
        "label": LEAD_TRAFEGO_LABEL_ID,
        "stage_id": STAGE_PRONTO_PROSPECCAO,
        "whatsapp_sent": False,
    }
