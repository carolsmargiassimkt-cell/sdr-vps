from fastapi import APIRouter, Request, Header, HTTPException
import os, requests

router = APIRouter()

TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN")
PD = os.getenv("PIPEDRIVE_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE = "https://api.pipedrive.com/v1"

WARM_WHATSAPP_LABEL_ID = 226

def pd(method, path, **kwargs):
    params = kwargs.pop("params", {})
    params["api_token"] = PD
    r = requests.request(method, BASE + path, params=params, timeout=20, **kwargs)
    print("[PD]", method, path, r.status_code, r.text[:200])
    if r.status_code >= 400:
        return None
    return r.json().get("data")

@router.post("/webhooks/forms-lead")
async def forms_lead(req: Request, authorization: str = Header(None)):
    if not TOKEN or authorization != f"Bearer {TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")

    body = await req.json()

    nome = (body.get("nome") or "Lead Forms").strip()
    email = (body.get("email") or "").strip()
    whatsapp = (body.get("whatsapp") or "").strip()
    segmento = body.get("segmento") or ""
    unidades = body.get("unidades") or ""
    campanhas = body.get("campanhas") or ""
    objetivo = body.get("objetivo") or ""

    print("[FORMS_RECEBIDO]", nome, email, whatsapp)

    person = pd("POST", "/persons", json={
        "name": nome,
        "email": [{"value": email, "primary": True, "label": "work"}] if email else [],
        "phone": [{"value": whatsapp, "primary": True, "label": "whatsapp"}] if whatsapp else [],
    })

    if not person:
        return {"ok": False, "error": "person_fail"}

    pid = person.get("id")

    deal = pd("POST", "/deals", json={
        "title": f"FORM - {nome}",
        "person_id": pid,
        "label": WARM_WHATSAPP_LABEL_ID,
    })

    if not deal:
        return {"ok": False, "error": "deal_fail", "person_id": pid}

    deal_id = deal.get("id")

    note = f"""
<b>Formulário respondido</b><br>
<b>Nome:</b> {nome}<br>
<b>Email:</b> {email}<br>
<b>WhatsApp:</b> {whatsapp}<br>
<b>Segmento:</b> {segmento}<br>
<b>Unidades:</b> {unidades}<br>
<b>Campanhas:</b> {campanhas}<br>
<b>Objetivo:</b> {objetivo}<br>
<b>Ação:</b> lead marcado como WARM_WHATSAPP.
"""
    pd("POST", "/notes", json={"deal_id": deal_id, "content": note})

    wa_ok = False
    if whatsapp:
        try:
            r = requests.post("http://127.0.0.1:3000/send", json={
                "number": whatsapp,
                "text": f"Oi {nome}, vi que você respondeu o formulário. Posso te explicar rapidamente como funciona?"
            }, timeout=10)
            wa_ok = r.status_code < 400
            print("[WA_SEND]", r.status_code, r.text[:200])
        except Exception as e:
            print("[WA_FAIL]", repr(e))

    return {"ok": True, "deal_id": deal_id, "person_id": pid, "whatsapp_sent": wa_ok}
