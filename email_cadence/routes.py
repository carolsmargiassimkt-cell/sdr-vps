from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response
from crm.pipedrive_client import PipedriveClient
from email_cadence.engine import enqueue
from core.sdr_state import STAGE_PRONTO_PROSPECCAO, mark_warm, update_score, log_event
import json
from pathlib import Path
from datetime import datetime

router = APIRouter()
crm = PipedriveClient()

WARM_WHATSAPP_LABEL_ID = 226
PIXEL_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;"
)


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

    phones = p.get("phone") or []
    phone = next((item.get("value") for item in phones if item.get("value")), "")

    return enqueue(
        deal_id,
        email,
        p.get("name", ""),
        org_name or "",
        phone,
    )


@router.get("/t/{deal_id}/{step}")
async def track_click(deal_id: int, step: int, req: Request):
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
        crm.update_deal(int(deal_id), {"stage_id": STAGE_PRONTO_PROSPECCAO})
    except Exception as exc:
        print("[WARM_LABEL_FAIL]", deal_id, exc)
    try:
        mark_warm(deal_id, source="email_click", score_event="email_click")
    except Exception as exc:
        print("[STATE_WARM_FAIL]", deal_id, exc)
    try:
        crm.create_activity(
            deal_id=deal_id,
            subject="PRIORIDADE: ligar/WhatsApp - clique na campanha Copa",
            type="call",
            note="Lead clicou na cadencia. Priorizar ligacao/WhatsApp."
        )
    except Exception:
        pass
    log_event("EMAIL_CLICK", deal_id=deal_id, step=step)
    target = str(req.query_params.get("r") or "https://manddigital.com.br/").strip()
    return RedirectResponse(target)


@router.get("/o/{deal_id}/{step}.gif")
async def track_open(deal_id: int, step: int):
    try:
        update_score(deal_id, "", "email_open")
        log_event("EMAIL_OPEN", deal_id=deal_id, step=step)
    except Exception as exc:
        print("[EMAIL_OPEN_FAIL]", deal_id, exc)
    return Response(content=PIXEL_GIF, media_type="image/gif")
