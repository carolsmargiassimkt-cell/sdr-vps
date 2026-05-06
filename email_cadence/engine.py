MAX_EMAILS_PER_TICK=3
import os,json,time,requests,smtplib,ssl
from datetime import datetime, timedelta
from pathlib import Path

# --- CONFIGURAÇÃO ---
BASE_DIR = Path(__file__).resolve().parents[1]
DATA = BASE_DIR / "data" / "email_cadence_queue.json"
EVENTS = BASE_DIR / "data" / "email_cadence_events.json"
DATA.parent.mkdir(parents=True, exist_ok=True)

PD = os.getenv("PIPEDRIVE_API_TOKEN")
FROM = os.getenv("SMTP_FROM_EMAIL", "")

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465") or 465)
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

STEPS = [0, 2, 4, 7, 10, 14]
STOP_STATUSES = {"clicked_warm", "replied", "won", "opt_out", "wrong_contact", "done"}
# --------------------

def cta_html(x, step):
    deal_id = x.get("deal_id") or x.get("id")

    forms = f"http://191.252.184.140:8001/t/{deal_id}/{step}?r=https://docs.google.com/forms/d/1KWo-Z7uKflvpR0Ff9yxFJhyV-iFtm0v4xH3QKC9FRmo/viewform?usp=header"
    calendly = f"http://191.252.184.140:8001/t/{deal_id}/{step}?r=https://calendly.com/ana-manddigital/30min"

    return (
        f"<br><br>Se fizer sentido, você pode:<br><br>"
        f"<a href='{forms}' style='color:#2563eb;text-decoration:underline;font-weight:600;'>👉 Responder em 1 minuto</a><br><br>"
        f"<a href='{calendly}' style='color:#16a34a;text-decoration:underline;font-weight:600;'>📅 Ou agendar direto comigo</a>"
    )

def segment_context(empresa=""):
    e=(empresa or "").lower()
    if any(x in e for x in ["supermerc", "mercado", "atacad", "varej", "hortifruti", "fruta"]):
        return "varejo alimentar", "giro, ticket médio e recorrência", "campanhas de Copa no ponto de venda"
    if any(x in e for x in ["shopping", "mall", "boulevard", "center"]):
        return "shopping/centro comercial", "fluxo, permanência e ativação das lojas", "ações interativas nos corredores e lojas"
    if any(x in e for x in ["industr", "alimentos", "bebidas", "ltda", "sa "]):
        return "indústria/marca", "sell-out, canal e ativação de marca", "promoções digitais ligadas ao varejo"
    if any(x in e for x in ["farm", "drog"]):
        return "farmácias", "recorrência, ticket e relacionamento", "campanhas sazonais com dados do cliente"
    return "varejo", "vendas, engajamento e dados reais", "campanhas interativas"


def build_track_link(x, step):
    base = "http://191.252.184.140:8001"
    return f"{base}/t/{x.get('deal_id')}/{step}?r=https://docs.google.com/forms/d/1KWo-Z7uKflvpR0Ff9yxFJhyV-iFtm0v4xH3QKC9FRmo/viewform?usp=header"





def template(step, x):
    nome=(x.get("nome") or "").split(" ")[0].title() or "tudo bem"
    empresa=x.get("empresa") or "a empresa"
    seg,dor,gancho=segment_context(empresa)
    cta=cta_html(x, step)

    textos={
1:("dúvida rápida sobre campanhas",f"Oi {nome}, tudo bem?<br><br>Queria te fazer uma pergunta direta:<br><br>Hoje vocês usam campanhas promocionais para gerar venda ou o crescimento vem mais do fluxo natural?<br><br>Pergunto porque, em {seg}, a Copa costuma abrir uma janela boa para transformar campanha em venda + dados reais do cliente.{cta}<br><br>Abs,<br>Carol"),
2:("o que tenho visto no mercado",f"Oi {nome},<br><br>Estou vendo um padrão se repetir em empresas como {empresa}: campanhas geram movimento, mas nem sempre capturam dados úteis depois.<br><br>Com uma experiência simples — roleta, raspadinha ou prêmio instantâneo — dá para medir melhor {dor}.{cta}<br><br>Abs,<br>Carol"),
3:("Copa como gatilho",f"Oi {nome},<br><br>Pensando na Copa: vocês já têm alguma ação para aproveitar o aumento de atenção do público?<br><br>A gente tem trabalhado {gancho}, com mecânicas simples para gerar engajamento e dados próprios.{cta}<br><br>Abs,<br>Carol"),
4:("campanha que vira dado",f"Oi {nome},<br><br>Um ponto comum em {seg}: muita campanha gera exposição, mas pouca inteligência para o próximo passo.<br><br>Quando o cliente participa, dá para entender interesse, frequência, canal, região e intenção.{cta}<br><br>Abs,<br>Carol"),
5:("antes da Copa",f"Oi {nome},<br><br>Quem estrutura campanha antes da Copa captura mais valor durante o pico — movimento, base própria, dados e recorrência.<br><br>Depois que começa, normalmente vira correria operacional.{cta}<br><br>Abs,<br>Carol"),
6:("faz sentido ou encerro por aqui?",f"Oi {nome},<br><br>Prometo ser minha última mensagem 🙂<br><br>Campanhas interativas orientadas a dados fazem sentido para {empresa} agora ou não é prioridade?<br><br>Se não fizer sentido, me fala que encerro por aqui sem problema.{cta}<br><br>Abs,<br>Carol")
}
    return textos.get(int(step), textos[1])

def pd_request(method,path,json_body=None):
    if not PD: return None
    url=f"https://api.pipedrive.com/v1/{path.lstrip('/')}"
    r=requests.request(method,url,params={"api_token":PD},json=json_body,timeout=30)
    print("[PD]",method,path,r.status_code)
    return r.json() if r.text else {}

def add_deal_note(deal_id,content):
    return pd_request("POST","notes",{"deal_id":int(deal_id),"content":content})

def add_activity(deal_id,subject):
    due=datetime.now().strftime("%Y-%m-%d")
    return pd_request("POST","activities",{
      "deal_id":int(deal_id),
      "subject":subject,
      "type":"call",
      "due_date":due,
      "note":"Lead clicou na cadência. Priorizar ligação/WhatsApp."
    })

EMAIL_LABEL_IDS = {
    "EMAIL_CAD1": 197,
    "EMAIL_CAD2": 198,
    "EMAIL_CAD3": 199,
    "EMAIL_CAD4": 200,
    "EMAIL_CAD5": 201,
    "EMAIL_CAD6": 219,
}

def add_deal_label(deal_id,label):
    label_id = EMAIL_LABEL_IDS.get(str(label))
    if label_id:
        pd_request("PUT", f"deals/{int(deal_id)}", {"label": str(label_id)})
        add_deal_note(deal_id,f"TAG/CADÊNCIA aplicada: {label}")
    else:
        add_deal_note(deal_id,f"TAG_SUGERIDA: {label}")

def load():
    return json.loads(DATA.read_text()) if DATA.exists() else []

def save(rows):
    DATA.write_text(json.dumps(rows,ensure_ascii=False,indent=2))

def load_events():
    if not EVENTS.exists():
        save_events([])
        return []
    try:
        with EVENTS.open("r",encoding="utf-8") as f:
            payload=json.load(f)
    except Exception:
        return []
    return payload if isinstance(payload,list) else []

def save_events(rows):
    EVENTS.parent.mkdir(parents=True,exist_ok=True)
    with EVENTS.open("w",encoding="utf-8") as f:
        json.dump(rows if isinstance(rows,list) else [],f,ensure_ascii=False,indent=2)

def now():
    return datetime.now().isoformat(timespec="seconds")

def idempotency_key(deal_id,step):
    return f"{int(deal_id)}:{int(step)}"

def event_exists(deal_id,step):
    key=idempotency_key(deal_id,step)
    for ev in load_events():
        if str(ev.get("idempotency_key") or "")==key:
            return True
    return False

def record_sent_event(x,step,subject,sent_at):
    key=idempotency_key(x.get("deal_id"),step)
    events=load_events()
    if any(str(ev.get("idempotency_key") or "")==key for ev in events):
        return False
    events.append({
      "deal_id":int(x.get("deal_id")),
      "email":str(x.get("email") or "").strip().lower(),
      "step":int(step),
      "subject":str(subject or ""),
      "sent_at":sent_at,
      "status":"sent",
      "idempotency_key":key
    })
    save_events(events)
    print("[EVENT_LOGGED]",key)
    return True

def advance_after_send(x,step,t):
    if step>=len(STEPS):
        x["status"]="done"
        x["next_send"]=None
    else:
        x["step"]=step+1
        x["status"]="pending"
        x["next_send"]=(t+timedelta(days=STEPS[step])).isoformat(timespec="seconds")

def enqueue(deal_id,email,nome="",empresa=""):
    rows=load()
    if any(int(x.get("deal_id",0))==int(deal_id) for x in rows):
        return {"ok":True,"skip":"already_queued"}
    rows.append({
      "deal_id":int(deal_id),"email":email,"nome":nome,"empresa":empresa,
      "step":1,"status":"pending","next_send":now(),"created_at":now()
    })
    save(rows)
    return {"ok":True,"queued":deal_id}


def send_smtp(to, subject, body):
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, FROM]):
        print("[DRY_RUN] SMTP env ausente:", to, subject)
        return {"ok":True,"dry_run":True}

    from email.message import EmailMessage
    msg = EmailMessage()
    msg["From"] = FROM
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body.replace("<br>", "<br>").replace("<br/>", "<br>"))
    msg.add_alternative(body, subtype="html")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=30) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

    print("[SMTP_SEND_OK]", to, subject)
    return {"ok":True,"dry_run":False}

def smtp_ok(result):
    if isinstance(result,dict):
        return bool(result.get("ok"))
    return bool(result)

def smtp_dry_run(result):
    return isinstance(result,dict) and bool(result.get("dry_run"))

def tick():
    rows=load(); changed=False
    t=datetime.now()
    sent_count=0
    for x in rows:
        if sent_count >= MAX_EMAILS_PER_TICK:
            print(f"[CADENCE_LIMIT_REACHED] max={MAX_EMAILS_PER_TICK}")
            break
        if str(x.get("status") or "") in STOP_STATUSES:
            continue

        if x.get("status")!="pending": continue
        if datetime.fromisoformat(x["next_send"])>t: continue
        step=int(x.get("step",1))
        subj,body=template(step, x)
        if event_exists(x.get("deal_id"),step):
            advance_after_send(x,step,t)
            changed=True
            continue
        result=send_smtp(x["email"],subj,body)
        if smtp_ok(result):
            if smtp_dry_run(result):
                print(f"[SKIP_ADVANCE_DRY_RUN] deal={x.get('deal_id')} email={x.get('email')} step={step}")
                continue
            sent_at=now()
            x["last_sent_at"]=sent_at
            record_sent_event(x,step,subj,sent_at)
            advance_after_send(x,step,t)
            sent_count += 1
            changed=True
    if changed: save(rows)
    print("[CADENCE_TICK_OK]")
if __name__=="__main__":
    tick()
