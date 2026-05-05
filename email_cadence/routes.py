from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from crm.pipedrive_client import PipedriveClient
from email_cadence.engine import enqueue
import json
from pathlib import Path
from datetime import datetime

router = APIRouter()
crm = PipedriveClient()

WARM_WHATSAPP_LABEL_ID = 226


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


def merge_label(deal_id, label_id):
    deal = crm.get_deal_details(int(deal_id)) or {}
    current = label_items(deal.get("label"))
    keys = {label_key(item) for item in current if label_key(item)}
    if str(label_id) not in keys:
        current.append(int(label_id))
    return crm.update_deal(int(deal_id), {"label": current})


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


@router.get("/t/{deal_id}/{step}")
async def track_click(deal_id: int, step: int):
    p = Path("/root/sdr-vps/data/email_cadence_queue.json")
    rows = json.loads(p.read_text()) if p.exists() else []
    for x in rows:
        if int(x.get("deal_id", 0)) == int(deal_id):
            x["clicked_at"] = datetime.now().isoformat(timespec="seconds")
            x["clicked_step"] = int(step)
            x["status"] = "clicked_warm"
    p.write_text(json.dumps(rows, ensure_ascii=False, indent=2))

    try:
        crm.add_note(deal_id=deal_id, content=f"CLIQUE detectado no email cadencia {step}. Lead virou warm.")
    except Exception:
        pass
    try:
        merge_label(deal_id, WARM_WHATSAPP_LABEL_ID)
    except Exception as exc:
        print("[WARM_LABEL_FAIL]", deal_id, exc)
    try:
        crm.create_activity(
            deal_id=deal_id,
            subject="PRIORIDADE: ligar/WhatsApp - clique na campanha Copa",
            type="call",
            note="Lead clicou na cadencia. Priorizar ligacao/WhatsApp."
        )
    except Exception:
        pass
    return RedirectResponse("https://manddigital.com.br/")
