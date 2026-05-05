from fastapi import APIRouter, Request
from crm.pipedrive_client import PipedriveClient
from email_cadence.engine import enqueue

router = APIRouter()
crm = PipedriveClient()

@router.post("/webhooks/email-cadence")
async def start_email_cadence(req: Request):
    body = await req.json()

    deal_id = int((body.get("current") or {}).get("id") or body.get("deal_id") or 0)
    if not deal_id:
        return {"ok": False, "error": "missing_deal_id"}

    deal = crm.get_deal_details(deal_id) or {}

    person = deal.get("person_id") or {}
    pid = person.get("value") or person.get("id") if isinstance(person, dict) else 0

    p = crm.get_person_details(pid) if pid else {}

    email = (
        body.get("email")
        or body.get("person_email")
        or body.get("to_email")
        or body.get("mail")
        or ""
    )

    if not email:
        emails = p.get("email") or []
        email = next((e.get("value") for e in emails if e.get("value")), "")

    if not email:
        return {"ok": True, "skip": "sem_email"}

    org = deal.get("org_id") or {}
    org_name = org.get("name") if isinstance(org, dict) else ""

    return enqueue(
        deal_id,
        email,
        p.get("name", ""),
        org_name or ""
    )

from fastapi.responses import RedirectResponse
import json
from pathlib import Path
try:
    from crm.pipedrive_client import add_deal_note, add_activity, add_deal_label
except ImportError:
    # Compatibilidade: se o client não expuser helpers funcionais,
    # mantém o webhook vivo e apenas loga, sem derrubar a API.
    def add_deal_note(*args, **kwargs):
        print("[CRM_HELPER_MISSING] add_deal_note indisponivel")
        return None

    def add_activity(*args, **kwargs):
        print("[CRM_HELPER_MISSING] add_activity indisponivel")
        return None

    def add_deal_label(*args, **kwargs):
        print("[CRM_HELPER_MISSING] add_deal_label indisponivel")
        return None

@router.get("/t/{deal_id}/{step}")
async def track_click(deal_id:int, step:int):
    p=Path("/root/sdr-vps/data/email_cadence_queue.json")
    rows=json.loads(p.read_text()) if p.exists() else []
    for x in rows:
        if int(x.get("deal_id",0))==deal_id:
            x["clicked_at"]=__import__("datetime").datetime.now().isoformat(timespec="seconds")
            x["clicked_step"]=step
            x["status"]="clicked_warm"
    p.write_text(json.dumps(rows,ensure_ascii=False,indent=2))
    add_deal_note(deal_id,f"CLIQUE detectado no email cadência {step}. Lead virou warm.")
    add_deal_label(deal_id,"warm_whatsapp")
    add_activity(deal_id,"PRIORIDADE: ligar/WhatsApp - clique na campanha Copa")
    return RedirectResponse("https://manddigital.com.br/")
