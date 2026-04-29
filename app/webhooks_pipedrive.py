from fastapi import APIRouter, Request
import os, requests

router = APIRouter()

PD_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
FIELD_KEY = "fc886c608c178c015703f86d05a31d0ec32ca754"

WA_CAD1_ENVIAR = 222
WA_CAD1_ENVIADO = 223

def pd_get(path):
    return requests.get(f"https://api.pipedrive.com/v1/{path}?api_token={PD_TOKEN}", timeout=20).json().get("data") or {}

def pd_put(path, payload):
    return requests.put(f"https://api.pipedrive.com/v1/{path}?api_token={PD_TOKEN}", json=payload, timeout=20)

def send_wa(phone, text):
    return requests.post("http://127.0.0.1:3000/send", json={"number": phone, "text": text}, timeout=40)

@router.post("/webhooks/pipedrive")
async def pipedrive_webhook(req: Request):
    body = await req.json()
    try:
        deal_id = (body.get("current") or {}).get("id")
        if not deal_id:
            return {"ok": True, "skip": "no_deal_id"}

        deal = pd_get(f"deals/{deal_id}")
        status = deal.get(FIELD_KEY)

        if str(status) != str(WA_CAD1_ENVIAR):
            return {"ok": True, "skip": f"status_{status}"}

        person_id = ((deal.get("person_id") or {}).get("value")) or deal.get("person_id")
        if not person_id:
            return {"ok": True, "skip": "no_person"}

        person = pd_get(f"persons/{person_id}")
        phones = person.get("phone") or []
        phone = phones[0].get("value") if phones else ""

        if not phone:
            return {"ok": True, "skip": "no_phone"}

        msg = "Oi, tudo bem? Vi que você interagiu com nosso conteúdo e queria falar contigo."

        send_wa(phone, msg)
        pd_put(f"deals/{deal_id}", {FIELD_KEY: WA_CAD1_ENVIADO})

        return {"ok": True, "deal_id": deal_id, "phone": phone}

    except Exception as e:
        print("[PD_WEBHOOK_ERRO]", repr(e))
        return {"ok": False, "error": str(e)}
