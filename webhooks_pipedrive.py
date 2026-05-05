from fastapi import APIRouter, Request

router = APIRouter()


@router.post("/webhooks/pipedrive")
async def webhook(req: Request):
    body = await req.json()
<<<<<<< HEAD
    deal_id = (body.get("current") or {}).get("id")
    print("[WA_WEBHOOK_ROOT_DISABLED]", deal_id or "-")
    return {"ok": True, "sent": False, "skip": "root_webhook_disabled_use_scripts_whatsapp_warm_cadence"}
=======

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
>>>>>>> 5c858a8be7eb428553fe3b537420d1f403328f7b
