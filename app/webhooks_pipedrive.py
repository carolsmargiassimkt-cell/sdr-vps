from fastapi import APIRouter, Request
import os, re, json, requests
from pathlib import Path

router = APIRouter()
PD_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
FIELD_KEY = "fc886c608c178c015703f86d05a31d0ec32ca754"

WA_CAD1_ENVIAR = 222
WA_CAD1_ENVIADO = 223
SENT_FILE = Path("/root/sdr-vps/data/wa_webhook_sent.json")
SENT_FILE.parent.mkdir(parents=True, exist_ok=True)

MSG = "Oi, tudo bem? Vi que voce interagiu com nosso conteudo e queria falar contigo rapidamente."


def load_sent():
    try:
        return json.loads(SENT_FILE.read_text())
    except Exception:
        return {}


def save_sent(d):
    SENT_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2))


def clean_phone(p):
    n = re.sub(r"\D+", "", str(p or ""))
    if len(n) in (10, 11):
        n = "55" + n
    return n


def pd_get(path):
    return requests.get(f"https://api.pipedrive.com/v1/{path}?api_token={PD_TOKEN}", timeout=20).json().get("data") or {}


def pd_put(path, payload):
    return requests.put(f"https://api.pipedrive.com/v1/{path}?api_token={PD_TOKEN}", json=payload, timeout=20)


def add_note(deal_id, text):
    requests.post(f"https://api.pipedrive.com/v1/notes?api_token={PD_TOKEN}", json={"deal_id": deal_id, "content": text}, timeout=20)


@router.post("/webhooks/pipedrive")
async def pipedrive_webhook(req: Request):
    body = await req.json()
    deal_id = (body.get("current") or {}).get("id")
    if not deal_id:
        return {"ok": True, "skip": "no_deal"}

    deal = pd_get(f"deals/{deal_id}")
    if str(deal.get(FIELD_KEY)) != str(WA_CAD1_ENVIAR):
        return {"ok": True, "skip": f"status_{deal.get(FIELD_KEY)}"}

    sent = load_sent()
    if str(deal_id) in sent:
        return {"ok": True, "skip": "already_sent"}

    person_id = ((deal.get("person_id") or {}).get("value")) or deal.get("person_id")
    person = pd_get(f"persons/{person_id}") if person_id else {}
    phones = person.get("phone") or []
    phone = clean_phone(phones[0].get("value") if phones else "")
    if not phone:
        return {"ok": True, "skip": "no_phone"}

    add_note(deal_id, f"[WA BLOQUEADO | CAD1]\nEnvio automatico direto bloqueado pelo patch de contencao.\nTelefone: {phone}")
    return {"ok": True, "sent": False, "deal_id": deal_id, "skip": "auto_blocked"}
