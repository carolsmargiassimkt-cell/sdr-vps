
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

router = APIRouter()

load_dotenv("/root/sdr-vps/.env", override=True)
load_dotenv("/root/sdr-vps/.env", override=True)
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
    token = (os.getenv("WEBHOOK_AUTH_TOKEN") or TOKEN or "").strip()
    if not token or (authorization or "").strip() != f"Bearer {token}":
        print("[FORMS_AUTH_FAIL]", "auth=", authorization, "token_loaded=", bool(token))
        raise HTTPException(status_code=401, detail="unauthorized")

    body = await req.json()

    nome = clean_crm_name(body.get("nome") or body.get("name") or body.get("title") or body.get("mensagem") or body.get("message") or "Lead Forms", body.get("email"))
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
