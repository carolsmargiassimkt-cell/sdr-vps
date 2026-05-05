
import re

def clean_crm_name(raw, email=""):
    txt = re.sub(r"<br\s*/?>", "\n", str(raw or ""), flags=re.I)
    txt = re.sub(r"<[^>]+>", "", txt).strip()

    # Ex: Lead Email - Cedar Plaza
    m = re.search(r"Lead Email\s*-\s*([^\n\r<]+)", txt, flags=re.I)
    if m:
        return m.group(1).strip()

    # se vier texto gigante do form, tenta primeira linha útil
    for line in txt.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith(("mensagem:", "data:", "horário:", "horario:", "url da página", "agente de usu")):
            continue
        return line[:80]

    if email and "@" in email:
        return email.split("@")[0].replace(".", " ").replace("_", " ").title()

    return "Lead sem nome"

from fastapi import APIRouter, Request, Header, HTTPException
import os
<<<<<<< HEAD
import re
import requests
=======
import requests
import time
from dotenv import load_dotenv


def clean_lead_name(name, email=""):
    n = (name or "").strip()
    if n.lower().startswith("lead email -"):
        n = n.split("-", 1)[-1].strip()
    bad = ("lead email", "lead form", "formulário", "formulario")
    if not n or any(x in n.lower() for x in bad):
        n = (email or "").split("@")[0].replace(".", " ").replace("_", " ").title()
    return n or "Lead sem nome"
>>>>>>> 5c858a8be7eb428553fe3b537420d1f403328f7b

router = APIRouter()

load_dotenv("/root/sdr-vps/.env", override=True)
load_dotenv("/root/sdr-vps/.env", override=True)
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
    token = (os.getenv("WEBHOOK_AUTH_TOKEN") or TOKEN or "").strip()
    if not token or (authorization or "").strip() != f"Bearer {token}":
        print("[FORMS_AUTH_FAIL]", "auth=", authorization, "token_loaded=", bool(token))
        raise HTTPException(status_code=401, detail="unauthorized")

    body = await req.json()

<<<<<<< HEAD
    nome = (body.get("nome") or body.get("name") or "Lead Forms").strip()
    email = (body.get("email") or "").strip().lower()
    whatsapp = (body.get("whatsapp") or body.get("telefone") or body.get("phone") or "").strip()
    empresa = (body.get("empresa") or body.get("company") or "").strip()
=======
    nome = clean_crm_name(body.get("nome") or body.get("name") or body.get("title") or body.get("mensagem") or body.get("message") or "Lead Forms", body.get("email"))
    email = (body.get("email") or "").strip()
    whatsapp = (body.get("whatsapp") or "").strip()
>>>>>>> 5c858a8be7eb428553fe3b537420d1f403328f7b
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

<<<<<<< HEAD
    return {
        "ok": True,
        "deal_id": deal_id,
        "person_id": pid,
        "org_id": org_id,
        "label": LEAD_TRAFEGO_LABEL_ID,
        "stage_id": STAGE_PRONTO_PROSPECCAO,
        "whatsapp_sent": False,
    }
=======
    wa_ok = False
    if whatsapp:
        try:
            numero = whatsapp
            if numero and not numero.startswith("55"):
                numero = "55" + numero

            msg1 = f"Oi {nome.split()[0].title() if nome else 'tudo bem'}, tudo bem? Aqui é a Carol da Mand Digital 🙂\n\nVi sua resposta no formulário — obrigada!"

            msg2 = f"Pelo que você comentou sobre {objetivo or 'gerar mais resultado'}, já dá pra ver um caminho interessante aí.\n\nPara empresas do segmento {segmento or 'varejo'} como {body.get('empresa') or 'a sua empresa'}, a Copa costuma ser uma janela bem forte pra transformar campanha em fluxo, venda e dados reais do cliente.\n\nA gente tem feito isso com algumas redes e tem dado bastante resultado.\n\nPosso te mandar um exemplo rápido por aqui?"

            r1 = requests.post("http://127.0.0.1:3000/send", json={
                "number": numero,
                "text": msg1
            }, timeout=30)
            print("[WA_SEND_1]", r1.status_code, r1.text[:200])

            time.sleep(4)

            r2 = requests.post("http://127.0.0.1:3000/send", json={
                "number": numero,
                "text": msg2
            }, timeout=30)
            print("[WA_SEND_2]", r2.status_code, r2.text[:200])

            wa_ok = r1.status_code < 400 and r2.status_code < 400
        except Exception as e:
            print("[WA_FAIL]", repr(e))

    return {"ok": True, "deal_id": deal_id, "person_id": pid, "whatsapp_sent": wa_ok}
>>>>>>> 5c858a8be7eb428553fe3b537420d1f403328f7b
