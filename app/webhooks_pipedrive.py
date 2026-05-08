from fastapi import APIRouter, Request
import os, re, json, requests
from pathlib import Path
from core.sdr_temperature import ensure_pronto_call_activity
from core.pipedrive_safe import safe_pd_request, merge_deal_labels

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
    return (safe_pd_request("GET", path) or {}).get("data") or {}


def pd_put(path, payload):
    if str(path or "").startswith("deals/") and "label" in dict(payload or {}):
        try:
            return merge_deal_labels(int(str(path).split("/", 1)[1]), list((payload or {}).get("label") or []))
        except Exception:
            pass
    return safe_pd_request("PUT", path, payload)


def add_note(deal_id, text):
    safe_pd_request("POST", "/notes", {"deal_id": deal_id, "content": text})


@router.post("/webhooks/pipedrive")
async def pipedrive_webhook(req: Request):
    body = await req.json()
    deal_id = (body.get("current") or {}).get("id")
    if not deal_id:
        return {"ok": True, "skip": "no_deal"}

    deal = pd_get(f"deals/{deal_id}")
    try:
        ensure_pronto_call_activity(deal, dry_run=False)
    except Exception as exc:
        print("[CALL_ACTIVITY_CREATE_FAIL]", deal_id, exc)

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

    add_note(deal_id, f"[WA BLOQUEADO | CAD1]\nEnvio automatico direto bloqueado. Emissor oficial: scripts/whatsapp_warm_cadence.py.\nTelefone: {phone}")
    sent[str(deal_id)] = {"sent": False, "reason": "auto_blocked", "phone": phone}
    save_sent(sent)
    return {"ok": True, "sent": False, "deal_id": deal_id, "skip": "auto_blocked"}
