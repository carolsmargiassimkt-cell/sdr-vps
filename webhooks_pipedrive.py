from fastapi import APIRouter, Request
import requests
import os

router = APIRouter()

PD_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")

FIELD_KEY = "fc886c608c178c015703f86d05a31d0ec32ca754"

WA_CAD1_ENVIAR = 222
WA_CAD1_ENVIADO = 223

def get_deal(deal_id):
    url = f"https://api.pipedrive.com/v1/deals/{deal_id}?api_token={PD_TOKEN}"
    return requests.get(url).json().get("data", {})

def get_person(person_id):
    url = f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PD_TOKEN}"
    return requests.get(url).json().get("data", {})

def update_deal(deal_id, value):
    url = f"https://api.pipedrive.com/v1/deals/{deal_id}?api_token={PD_TOKEN}"
    requests.put(url, json={FIELD_KEY: value})

def send_whatsapp(phone, message):
    requests.post("http://127.0.0.1:3000/send", json={
        "number": phone,
        "text": message
    })

@router.post("/webhooks/pipedrive")
async def webhook(req: Request):
    body = await req.json()

    try:
        deal_id = body.get("current", {}).get("id")
        if not deal_id:
            return {"ok": True}

        deal = get_deal(deal_id)
        status = str(deal.get(FIELD_KEY) or "").strip().lower()
        
labels = deal.get("label") or []
labels_str = str(labels).lower()

        stage_id = int(deal.get("stage_id") or 0)

        WARM_VALUES = {"warm_whatsapp", "acionar_whatsapp", "whatsapp_warm", "mailchimp_click"}

        if status not in WARM_VALUES and not ("226" in labels_str):
            return {"ok": True, "skip": "not_warm", "status": status, "labels": labels, "stage_id": stage_id}

        if status == WA_CAD1_ENVIAR:

            person_id = deal.get("person_id", {}).get("value")
            if not person_id:
                return {"ok": True}

            person = get_person(person_id)

            phone = None
            if person.get("phone"):
                phone = person["phone"][0]["value"]

            if not phone:
                return {"ok": True}

            msg = "Oi, tudo bem? Vi que você interagiu com nosso conteúdo e queria falar contigo."

            send_whatsapp(phone, msg)

            update_deal(deal_id, WA_CAD1_ENVIADO)

        return {"ok": True}

    except Exception as e:
        print("[ERRO_WEBHOOK]", e)
        return {"ok": False}
