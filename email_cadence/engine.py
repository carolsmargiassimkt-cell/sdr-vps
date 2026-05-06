from __future__ import annotations

import json
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

import requests

from core.crm_hygiene import is_generic_email
from core.sdr_state import mark_email_cadence, log_event


BASE_DIR = Path(__file__).resolve().parents[1]
DATA = BASE_DIR / "data" / "email_cadence_queue.json"
EVENTS = BASE_DIR / "data" / "email_cadence_events.json"
DATA.parent.mkdir(parents=True, exist_ok=True)

PD = os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN") or ""
FROM = os.getenv("SMTP_FROM_EMAIL", "")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465") or 465)
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
TRACKING_BASE_URL = os.getenv("TRACKING_BASE_URL", "http://191.252.184.140:8001").rstrip("/")
FORM_URL = os.getenv("PREFORM_URL", "https://docs.google.com/forms/d/1KWo-Z7uKflvpR0Ff9yxFJhyV-iFtm0v4xH3QKC9FRmo/viewform?usp=header")
CALENDLY_URL = os.getenv("CALENDLY_URL", "https://calendly.com/ana-manddigital/30min")

STEPS = [0, 2, 4, 7, 10, 14]
STOP_STATUSES = {"clicked_warm", "replied", "won", "opt_out", "wrong_contact", "done", "stopped", "warm"}
DAILY_LIMIT = int(os.getenv("EMAIL_DAILY_LIMIT", "50") or "50")
MIN_DELAY_SECONDS = float(os.getenv("EMAIL_MIN_DELAY_SECONDS", "45") or "45")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        return payload if payload is not None else default
    except Exception:
        return default


def save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def cta_html(x, step):
    deal_id = x.get("deal_id") or x.get("id")
    forms = f"{TRACKING_BASE_URL}/t/{deal_id}/{step}?r={FORM_URL}"
    calendly = f"{TRACKING_BASE_URL}/t/{deal_id}/{step}?r={CALENDLY_URL}"
    return (
        "<br><br>Se fizer sentido, voce pode:<br><br>"
        f"<a href='{forms}' style='color:#2563eb;text-decoration:underline;font-weight:600;'>Responder em 1 minuto</a><br><br>"
        f"<a href='{calendly}' style='color:#16a34a;text-decoration:underline;font-weight:600;'>Ou agendar direto comigo</a>"
    )


def open_pixel(x, step):
    deal_id = x.get("deal_id") or x.get("id")
    return f"<img src='{TRACKING_BASE_URL}/o/{deal_id}/{step}.gif' width='1' height='1' style='display:none' alt='' />"


def segment_context(empresa=""):
    e = (empresa or "").lower()
    if any(x in e for x in ["supermerc", "mercado", "atacad", "varej", "hortifruti", "fruta"]):
        return "varejo alimentar", "giro, ticket medio e recorrencia", "campanhas no ponto de venda"
    if any(x in e for x in ["shopping", "mall", "boulevard", "center"]):
        return "shopping/centro comercial", "fluxo, permanencia e ativacao das lojas", "acoes interativas nos corredores e lojas"
    if any(x in e for x in ["industr", "alimentos", "bebidas", "ltda", "sa "]):
        return "industria/marca", "sell-out, canal e ativacao de marca", "promocoes digitais ligadas ao varejo"
    if any(x in e for x in ["farm", "drog"]):
        return "farmacias", "recorrencia, ticket e relacionamento", "campanhas sazonais com dados do cliente"
    return "varejo", "vendas, engajamento e dados reais", "campanhas interativas"


def template(step, x):
    nome = (x.get("nome") or "").split(" ")[0].title() or "tudo bem"
    empresa = x.get("empresa") or "a empresa"
    seg, dor, gancho = segment_context(empresa)
    cta = cta_html(x, step)
    pixel = open_pixel(x, step)
    textos = {
        1: ("duvida rapida sobre campanhas", f"Oi {nome}, tudo bem?<br><br>Queria te fazer uma pergunta direta:<br><br>Hoje voces usam campanhas promocionais para gerar venda ou o crescimento vem mais do fluxo natural?<br><br>Pergunto porque, em {seg}, existe uma janela boa para transformar campanha em venda + dados reais do cliente.{cta}<br><br>Abs,<br>Carol{pixel}"),
        2: ("o que tenho visto no mercado", f"Oi {nome},<br><br>Estou vendo um padrao se repetir em empresas como {empresa}: campanhas geram movimento, mas nem sempre capturam dados uteis depois.<br><br>Com uma experiencia simples, como roleta, raspadinha ou premio instantaneo, da para medir melhor {dor}.{cta}<br><br>Abs,<br>Carol{pixel}"),
        3: ("campanha como gatilho", f"Oi {nome},<br><br>Voces ja tem alguma acao para aproveitar datas fortes e aumentar participacao do publico?<br><br>A gente tem trabalhado {gancho}, com mecanicas simples para gerar engajamento e dados proprios.{cta}<br><br>Abs,<br>Carol{pixel}"),
        4: ("campanha que vira dado", f"Oi {nome},<br><br>Um ponto comum em {seg}: muita campanha gera exposicao, mas pouca inteligencia para o proximo passo.<br><br>Quando o cliente participa, da para entender interesse, frequencia, canal, regiao e intencao.{cta}<br><br>Abs,<br>Carol{pixel}"),
        5: ("antes da proxima campanha", f"Oi {nome},<br><br>Quem estrutura campanha antes da data comercial captura mais valor durante o pico: movimento, base propria, dados e recorrencia.<br><br>Depois que comeca, normalmente vira correria operacional.{cta}<br><br>Abs,<br>Carol{pixel}"),
        6: ("faz sentido ou encerro por aqui?", f"Oi {nome},<br><br>Prometo ser minha ultima mensagem.<br><br>Campanhas interativas orientadas a dados fazem sentido para {empresa} agora ou nao e prioridade?<br><br>Se nao fizer sentido, me fala que encerro por aqui sem problema.{cta}<br><br>Abs,<br>Carol{pixel}"),
    }
    return textos.get(int(step), textos[1])


def pd_request(method, path, json_body=None):
    if not PD:
        return None
    url = f"https://api.pipedrive.com/v1/{path.lstrip('/')}"
    r = requests.request(method, url, params={"api_token": PD}, json=json_body, timeout=30)
    log_event("CRM_STAGE_UPDATE", method=method, path=path, status=r.status_code)
    return r.json() if r.text else {}


def load():
    rows = load_json(DATA, [])
    return rows if isinstance(rows, list) else []


def save(rows):
    save_json(DATA, rows if isinstance(rows, list) else [])


def load_events():
    rows = load_json(EVENTS, [])
    return rows if isinstance(rows, list) else []


def save_events(rows):
    save_json(EVENTS, rows if isinstance(rows, list) else [])


def now():
    return datetime.now().isoformat(timespec="seconds")


def idempotency_key(deal_id, step):
    return f"{int(deal_id)}:{int(step)}"


def event_exists(deal_id, step):
    key = idempotency_key(deal_id, step)
    return any(str(ev.get("idempotency_key") or "") == key for ev in load_events())


def record_sent_event(x, step, subject, sent_at):
    key = idempotency_key(x.get("deal_id"), step)
    events = load_events()
    if any(str(ev.get("idempotency_key") or "") == key for ev in events):
        return False
    events.append({
        "deal_id": int(x.get("deal_id")),
        "email": str(x.get("email") or "").strip().lower(),
        "step": int(step),
        "subject": str(subject or ""),
        "sent_at": sent_at,
        "status": "sent",
        "idempotency_key": key,
    })
    save_events(events)
    return True


def advance_after_send(x, step, t):
    if step >= len(STEPS):
        x["status"] = "done"
        x["next_send"] = None
    else:
        x["step"] = step + 1
        x["status"] = "pending"
        x["next_send"] = (t + timedelta(days=STEPS[step])).isoformat(timespec="seconds")


def enqueue(deal_id, email, nome="", empresa="", phone=""):
    if is_generic_email(email):
        log_event("EMAIL_QUEUE_SKIP", deal_id=deal_id, email=email, reason="generic_or_invalid_email")
        return {"ok": True, "skip": "generic_or_invalid_email"}
    rows = load()
    if any(int(x.get("deal_id", 0)) == int(deal_id) for x in rows):
        return {"ok": True, "skip": "already_queued"}
    row = {
        "deal_id": int(deal_id),
        "email": str(email or "").strip().lower(),
        "nome": nome,
        "empresa": empresa,
        "phone": phone,
        "step": 1,
        "status": "pending",
        "next_send": now(),
        "created_at": now(),
    }
    rows.append(row)
    save(rows)
    mark_email_cadence(deal_id, phone=phone, active=True, origin="outbound")
    log_event("EMAIL_QUEUE_ADD", deal_id=deal_id, email=row["email"])
    return {"ok": True, "queued": deal_id}


def send_smtp(to, subject, body):
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, FROM]):
        print("[DRY_RUN] SMTP env ausente:", to, subject)
        return {"ok": True, "dry_run": True}
    msg = EmailMessage()
    msg["From"] = FROM
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body.replace("<br>", "\n"))
    msg.add_alternative(body, subtype="html")
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=30) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
    return {"ok": True, "dry_run": False}


def smtp_ok(result):
    return bool(result.get("ok")) if isinstance(result, dict) else bool(result)


def smtp_dry_run(result):
    return isinstance(result, dict) and bool(result.get("dry_run"))


def tick():
    rows = load()
    changed = False
    t = datetime.now()
    sent_today = 0
    for x in rows:
        if sent_today >= DAILY_LIMIT:
            break
        if str(x.get("status") or "") in STOP_STATUSES:
            continue
        if x.get("status") != "pending":
            continue
        if datetime.fromisoformat(x["next_send"]) > t:
            continue
        step = int(x.get("step", 1))
        subj, body = template(step, x)
        if event_exists(x.get("deal_id"), step):
            advance_after_send(x, step, t)
            changed = True
            continue
        result = send_smtp(x["email"], subj, body)
        if smtp_ok(result):
            sent_at = now()
            x["last_sent_at"] = sent_at
            if not smtp_dry_run(result):
                record_sent_event(x, step, subj, sent_at)
                log_event("EMAIL_SENT", deal_id=x.get("deal_id"), email=x.get("email"), step=step, subject=subj)
                sent_today += 1
                time.sleep(MIN_DELAY_SECONDS)
            advance_after_send(x, step, t)
            changed = True
    if changed:
        save(rows)
    print("[CADENCE_TICK_OK]")


if __name__ == "__main__":
    tick()
