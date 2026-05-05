<<<<<<< HEAD
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

BASE_DIR = Path("/root/sdr-vps")
if not BASE_DIR.exists():
    BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"
STATE_FILE = DATA_DIR / "whatsapp_warm_cadence.json"
CONVERSATION_STATE_FILE = DATA_DIR / "whatsapp_conversation_state.json"
LOCK_FILE = DATA_DIR / "whatsapp_warm_cadence.lock"
EMAIL_QUEUE = DATA_DIR / "email_cadence_queue.json"
MANUAL_BLOCKLIST = DATA_DIR / "whatsapp_manual_blocklist.json"
LOGS_BLOCKLIST = BASE_DIR / "logs" / "whatsapp_manual_blocklist.json"
INVALIDOS = BASE_DIR / "invalidos.json"

PD_TOKEN = os.getenv("PIPEDRIVE_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
PD_BASE = "https://api.pipedrive.com/v1"
WA_BASE = os.getenv("WHATSAPP_GATEWAY_URL", "http://127.0.0.1:3000").rstrip("/")

PIPELINE_ID = 7
STAGE_ENTRADA = 51
STAGE_NUTRICAO = 62
STAGE_PRONTO_PROSPECCAO = 63
STAGE_TENTATIVA_CONTATO = 52

LEAD_TRAFEGO_LABEL_ID = 193
RESPONDIDO_LABEL_ID = 196
WARM_WHATSAPP_LABEL_ID = 226

STEP = "WA1"
MAX_SENDS = max(1, int(os.getenv("WARM_WA_BATCH_LIMIT", "20") or "20"))
MIN_DELAY = max(0, float(os.getenv("WARM_WA_MIN_DELAY_SEC", "8") or "8"))
MAX_DELAY = max(MIN_DELAY, float(os.getenv("WARM_WA_MAX_DELAY_SEC", "20") or "20"))
DRY_RUN = str(os.getenv("WARM_WA_DRY_RUN", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_MESSAGE = (
    "Oi, tudo bem? Aqui e a Carol da Mand Digital. "
    "Vi uma interacao sua com nosso conteudo e queria entender rapidinho se faz sentido conversar sobre campanhas interativas para gerar venda e dados reais. "
    "Quer que eu te mande um exemplo rapido?"
)


def load_json(path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if payload is not None else default
    except Exception:
        return default


def save_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def acquire_lock():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("ascii", errors="ignore"))
        os.close(fd)
        return True
    except FileExistsError:
        print("[WARM_WA_LOCKED]")
        return False


def release_lock():
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass


def pd(method, path, **kwargs):
    if not PD_TOKEN:
        print("[PD_SKIP] token ausente")
        return None
    params = kwargs.pop("params", {})
    params["api_token"] = PD_TOKEN
    response = requests.request(method, PD_BASE + path, params=params, timeout=30, **kwargs)
    if response.status_code >= 400:
        print("[PD_FAIL]", method, path, response.status_code, response.text[:300])
        return None
    return response.json().get("data") if response.text else {}


def normalize_phone(value):
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits


def phone_variants(phone):
    normalized = normalize_phone(phone)
    variants = {normalized}
    if normalized:
        variants.add("55" + normalized)
    return {item for item in variants if item}


def is_valid_phone(phone):
    normalized = normalize_phone(phone)
    return len(normalized) in (10, 11)


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
        return str(item.get("id") or item.get("label") or item.get("name") or item.get("value") or "").strip().upper()
    return str(item or "").strip().upper()


def label_tokens(deal):
    return {label_key(item) for item in label_items((deal or {}).get("label")) if label_key(item)}


def clicked_warm_deals():
    rows = load_json(EMAIL_QUEUE, [])
    if not isinstance(rows, list):
        return set()
    return {
        int(row.get("deal_id") or 0)
        for row in rows
        if str(row.get("status") or "").strip().lower() == "clicked_warm" and int(row.get("deal_id") or 0) > 0
    }


def load_blocked_numbers():
    blocked = set()
    for path in (MANUAL_BLOCKLIST, LOGS_BLOCKLIST, INVALIDOS):
        payload = load_json(path, [])
        items = payload.values() if isinstance(payload, dict) else payload
        for item in list(items or []):
            if isinstance(item, dict):
                raw = item.get("phone") or item.get("telefone") or item.get("number")
            else:
                raw = item
            for variant in phone_variants(raw):
                blocked.add(variant)
    return blocked


def state_key(deal_id, phone, step=STEP):
    return f"{int(deal_id)}:{normalize_phone(phone)}:{step}"


def conversation_key(deal_id, phone):
    return f"{int(deal_id)}:{normalize_phone(phone)}"


def normalize_origin(origin):
    value = str(origin or "").strip().lower()
    if value == "lead_trafego":
        return "lead_trafego"
    return "warm_email"


def already_sent(state, deal_id, phone, step=STEP):
    key = state_key(deal_id, phone, step)
    return key in state.get("sent", {})


def mark_sent(state, deal_id, phone, message, origin, step=STEP):
    state.setdefault("sent", {})[state_key(deal_id, phone, step)] = {
        "deal_id": int(deal_id),
        "phone": normalize_phone(phone),
        "origin": normalize_origin(origin),
        "step": step,
        "message": message,
        "sent_at": datetime.now().isoformat(timespec="seconds"),
    }


def load_conversation_state():
    payload = load_json(CONVERSATION_STATE_FILE, {})
    return payload if isinstance(payload, dict) else {}


def save_conversation_state(payload):
    save_json(CONVERSATION_STATE_FILE, payload if isinstance(payload, dict) else {})


def get_conversation_record(state, deal_id, phone):
    key = conversation_key(deal_id, phone)
    record = state.get(key)
    return record if isinstance(record, dict) else {}


def is_conversation_blocked(state, deal_id, phone):
    record = get_conversation_record(state, deal_id, phone)
    return bool(record.get("stopped") or record.get("replied") or record.get("wa1_sent"))


def mark_conversation_wa1_sent(state, deal_id, phone, origin, message):
    key = conversation_key(deal_id, phone)
    previous = state.get(key) if isinstance(state.get(key), dict) else {}
    now = datetime.now().isoformat(timespec="seconds")
    state[key] = {
        "deal_id": int(deal_id),
        "phone": normalize_phone(phone),
        "origin": normalize_origin(origin),
        "wa1_sent": True,
        "replied": bool(previous.get("replied", False)),
        "asked_case": bool(previous.get("asked_case", False)),
        "interested": bool(previous.get("interested", False)),
        "opt_out": bool(previous.get("opt_out", False)),
        "wrong_contact": bool(previous.get("wrong_contact", False)),
        "referred": bool(previous.get("referred", False)),
        "stopped": bool(previous.get("stopped", False)),
        "last_intent": str(previous.get("last_intent") or "wa1_sent"),
        "last_bot_reply": str(message or ""),
        "last_lead_reply": str(previous.get("last_lead_reply") or ""),
        "updated_at": now,
        "wa1_sent_at": str(previous.get("wa1_sent_at") or now),
    }


def is_stopped(state, deal_id, phone=""):
    stopped = state.get("stopped", {})
    if str(int(deal_id)) in stopped:
        return True
    if phone and f"phone:{normalize_phone(phone)}" in stopped:
        return True
    return False


def get_deals():
    deals = []
    start = 0
    while True:
        chunk = pd("GET", "/deals", params={
            "status": "open",
            "pipeline_id": PIPELINE_ID,
            "limit": 100,
            "start": start,
            "sort": "id DESC",
        })
        if not chunk:
            break
        deals.extend([dict(item) for item in chunk if isinstance(item, dict)])
        if len(chunk) < 100:
            break
        start += 100
    return deals


def person_id_from_deal(deal):
    person = (deal or {}).get("person_id")
    if isinstance(person, dict):
        return int(person.get("value") or person.get("id") or 0)
    return int(person or 0)


def phones_for_deal(deal):
    person_id = person_id_from_deal(deal)
    if not person_id:
        return []
    person = pd("GET", f"/persons/{person_id}") or {}
    phones = []
    for item in person.get("phone") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        normalized = normalize_phone(raw)
        if normalized and normalized not in phones:
            phones.append(normalized)
    return phones


def eligible_deal(deal, clicked_deal_ids):
    deal_id = int((deal or {}).get("id") or 0)
    if not deal_id:
        return False, "missing_deal_id"
    status = str((deal or {}).get("status") or "open").lower()
    if status in {"won", "lost", "deleted"}:
        return False, f"status_{status}"
    if int((deal or {}).get("pipeline_id") or PIPELINE_ID) != PIPELINE_ID:
        return False, "wrong_pipeline"
    stage_id = int((deal or {}).get("stage_id") or 0)
    tokens = label_tokens(deal)
    if str(RESPONDIDO_LABEL_ID) in tokens or "RESPONDIDO" in tokens:
        return False, "respondido"
    is_trafego = str(LEAD_TRAFEGO_LABEL_ID) in tokens or "LEAD_TR\u00c1FEGO" in tokens or "LEAD_TRAFEGO" in tokens
    is_warm = str(WARM_WHATSAPP_LABEL_ID) in tokens or "WARM_WHATSAPP" in tokens or deal_id in clicked_deal_ids
    if is_trafego and stage_id == STAGE_PRONTO_PROSPECCAO:
        return True, "lead_trafego"
    if is_warm and stage_id in {STAGE_ENTRADA, STAGE_NUTRICAO, STAGE_PRONTO_PROSPECCAO}:
        return True, "warm_email"
    return False, "not_allowed_origin_stage"


def build_message(deal, reason):
    configured = str(os.getenv("WARM_WA1_MESSAGE") or "").strip()
    if configured:
        return configured
    title = str((deal or {}).get("title") or "").strip()
    if reason == "lead_trafego":
        return "Oi, tudo bem? Aqui e a Carol da Mand Digital. Vi seu contato pelo formulario/LP/Leadster e queria entender rapidinho seu objetivo com campanhas interativas. Posso te mandar um exemplo?"
    if title:
        return f"Oi, tudo bem? Aqui e a Carol da Mand Digital. Vi uma interacao sua com nosso email/conteudo sobre {title} e achei que talvez fizesse sentido te mostrar uma ideia. Quer que eu te mande um exemplo rapido?"
    return DEFAULT_MESSAGE


def send_whatsapp(phone, message):
    number = "55" + normalize_phone(phone)
    response = requests.post(
        f"{WA_BASE}/send",
        json={"number": number, "text": message, "count_towards_daily_limit": True},
        timeout=45,
    )
    if response.status_code >= 400:
        print("[WA_SEND_FAIL]", phone, response.status_code, response.text[:200])
        return False
    try:
        payload = response.json()
    except Exception:
        payload = {}
    status = str(payload.get("status") or "").lower()
    ok = status == "sent"
    print("[WA_SEND_RESULT]", phone, status or response.status_code)
    return ok


def add_note(deal_id, content):
    return pd("POST", "/notes", json={"deal_id": int(deal_id), "content": content})


def move_stage(deal_id, stage_id):
    return pd("PUT", f"/deals/{int(deal_id)}", json={"stage_id": int(stage_id)})


def run():
    if not acquire_lock():
        return 0
    try:
        state = load_json(STATE_FILE, {})
        if not isinstance(state, dict):
            state = {}
        state.setdefault("sent", {})
        state.setdefault("stopped", {})
        state["last_run_at"] = datetime.now().isoformat(timespec="seconds")
        conversation_state = load_conversation_state()

        clicked_deal_ids = clicked_warm_deals()
        blocked = load_blocked_numbers()
        sent_count = 0

        for deal in get_deals():
            if sent_count >= MAX_SENDS:
                break
            deal_id = int(deal.get("id") or 0)
            ok, reason = eligible_deal(deal, clicked_deal_ids)
            if not ok:
                continue
            if is_stopped(state, deal_id):
                print("[WARM_WA_SKIP_STOPPED]", deal_id)
                continue
            for phone in phones_for_deal(deal):
                variants = phone_variants(phone)
                if not is_valid_phone(phone):
                    print("[WARM_WA_SKIP_INVALID]", deal_id, phone)
                    continue
                if variants & blocked:
                    print("[WARM_WA_SKIP_BLOCKLIST]", deal_id, phone)
                    continue
                if is_stopped(state, deal_id, phone):
                    print("[WARM_WA_SKIP_STOPPED_PHONE]", deal_id, phone)
                    continue
                if is_conversation_blocked(conversation_state, deal_id, phone):
                    print("[WARM_WA_SKIP_CONVERSATION_STATE]", deal_id, phone)
                    continue
                if already_sent(state, deal_id, phone):
                    print("[WARM_WA_SKIP_ALREADY_SENT]", deal_id, phone, STEP)
                    continue
                message = build_message(deal, reason)
                if DRY_RUN:
                    print("[WARM_WA_DRY_RUN]", deal_id, phone, reason, STEP)
                    continue
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                if not send_whatsapp(phone, message):
                    continue
                mark_sent(state, deal_id, phone, message, reason)
                mark_conversation_wa1_sent(conversation_state, deal_id, phone, reason, message)
                move_stage(deal_id, STAGE_TENTATIVA_CONTATO)
                add_note(
                    deal_id,
                    f"<b>WhatsApp {STEP} enviado</b><br><b>Origem:</b> {reason}<br><b>Telefone:</b> {phone}<br><b>Acao:</b> movido para Tentativa de Contato.",
                )
                sent_count += 1
                save_json(STATE_FILE, state)
                save_conversation_state(conversation_state)
                break

        state["last_result"] = {"sent": sent_count, "finished_at": datetime.now().isoformat(timespec="seconds")}
        save_json(STATE_FILE, state)
        print("[WARM_WA_DONE]", sent_count)
        return sent_count
    finally:
        release_lock()


if __name__ == "__main__":
    sys.exit(0 if run() >= 0 else 1)
=======
import os, re, json, time, requests
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv("/root/sdr-vps/.env")

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
API="https://api.pipedrive.com/v1"

PIPELINE_ID=7
STAGE_PRONTO=63
STAGE_TENTATIVA=52

LABEL_LEAD_TRAFEGO="193"
LABEL_WARM_WHATSAPP="226"
LABEL_RESPONDIDO="196"

STATE_FILE=Path("/root/sdr-vps/data/whatsapp_warm_cadence.json")
LOG_PREFIX="[WA_WARM_CADENCE]"

INTERVALS_DAYS={
    1:0,
    2:1,
    3:2,
    4:3,
    5:5,
    6:7,
}

MSG_TRAFEGO={
1:"""Oi, tudo bem? Aqui é a Carol, da Mand Digital 😊

Vi que você preencheu nosso formulário sobre promoção comercial/vale-brinde e quis te chamar por aqui pra entender melhor o que você está buscando.

Posso te explicar rapidinho como funciona?""",

2:"""Oi! Passando só pra ver se você conseguiu ver minha mensagem 😊

A ideia é bem simples: transformar uma campanha promocional em uma experiência interativa, tipo roleta, raspadinha ou vale-brinde, pra gerar participação e capturar dados dos clientes.

Faz sentido eu te mostrar um exemplo?""",

3:"""Carol da Mand passando por aqui rapidinho 😊

Esse tipo de ação costuma funcionar bem pra varejo, loja, mercado e serviços porque une promoção + cadastro + WhatsApp em uma experiência só.

Vocês estão pensando em alguma campanha específica agora?""",

4:"""Oi! Posso te mandar um exemplo de campanha simples?

Algo como: cliente compra, participa pelo QR Code/WhatsApp, gira uma roleta ou recebe um vale-brinde, e a empresa captura os dados para futuras ações.""",

5:"""Só pra eu não insistir errado 😊

Hoje vocês têm interesse em estruturar uma campanha promocional interativa ou ainda não é prioridade?""",

6:"""Tudo bem, vou encerrar por aqui pra não te incomodar 😊

Se em algum momento quiser uma ideia de promoção comercial, vale-brinde, roleta ou raspadinha digital, é só me chamar."""
}

MSG_WARM={
1:"""Oi, tudo bem? Aqui é a Carol, da Mand Digital 😊

Vi que você interagiu com nosso material sobre campanhas promocionais e quis te chamar por aqui.

Posso te explicar rapidinho como a Mand ajuda empresas a transformar promoções em experiências interativas?""",

2:"""Oi! Passando só pra ver se você conseguiu ver minha mensagem 😊

A ideia é usar campanhas como roleta, raspadinha, vale-brinde ou quiz pra gerar participação, venda e base de dados própria.

Faz sentido pra vocês?""",

3:"""Carol da Mand por aqui 😊

Esse tipo de campanha costuma ajudar quando a empresa quer vender mais em datas fortes e ainda capturar dados dos clientes.

Vocês têm alguma campanha prevista?""",

4:"""Posso te mandar uma ideia simples de aplicação?

Por exemplo: campanha de compra + participação pelo WhatsApp + premiação instantânea + captura de lead para próximas ações.""",

5:"""Só pra eu entender: isso faz sentido para vocês agora ou é algo mais para olhar depois?""",

6:"""Sem problema, vou encerrar por aqui pra não insistir 😊

Se quiser retomar a ideia de campanha promocional interativa, fico à disposição."""
}

def now():
    return datetime.now()

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}

def save_state(state):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))

def pd(method,path,json_body=None,params=None):
    params=params or {}
    params["api_token"]=TOKEN
    r=requests.request(method, API+path, params=params, json=json_body, timeout=30)
    r.raise_for_status()
    return r.json()

def phone_clean(v):
    nums=re.sub(r"\D","",v or "")
    if len(nums) in (10,11):
        nums="55"+nums
    return nums

def labels_of(deal):
    raw=deal.get("label")
    if raw is None:
        return set()
    if isinstance(raw, list):
        return {str(x) for x in raw}
    return {x.strip() for x in str(raw).split(",") if x.strip()}

def get_phone(deal):
    person=deal.get("person_id") or {}
    if isinstance(person,dict):
        for p in person.get("phone") or []:
            num=phone_clean(p.get("value"))
            if num:
                return num
    return ""

def get_open_deals():
    out=[]
    start=0
    while True:
        r=pd("GET","/deals",params={"status":"open","start":start,"limit":100})
        data=r.get("data") or []
        for d in data:
            if int(d.get("pipeline_id") or 0)!=PIPELINE_ID:
                continue
            if int(d.get("stage_id") or 0) not in {STAGE_PRONTO, STAGE_TENTATIVA}:
                continue
            labs=labels_of(d)
            if LABEL_RESPONDIDO in labs:
                continue
            if LABEL_LEAD_TRAFEGO in labs or LABEL_WARM_WHATSAPP in labs:
                # segurança: só deals criados depois de 2026-05-05 por enquanto
                if (d.get("add_time") or "")[:10] >= "2026-05-05":
                    out.append(d)
        pag=r.get("additional_data",{}).get("pagination",{})
        if not pag.get("more_items_in_collection"):
            break
        start += 100
    return out

def due_for_step(record, next_step):
    if next_step == 1:
        return True
    last=record.get("last_sent_at")
    if not last:
        return True
    last_dt=datetime.strptime(last,"%Y-%m-%d %H:%M:%S")
    return now() >= last_dt + timedelta(days=INTERVALS_DAYS.get(next_step, 99))

def send_wa(phone,text):
    r=requests.post("http://127.0.0.1:3000/send",json={"number":phone,"text":text},timeout=60)
    print(LOG_PREFIX,"WA",phone,r.status_code,r.text[:160])
    return r.ok and '"sent"' in r.text

def main(apply=False, send=False, limit=10):
    state=load_state()
    deals=get_open_deals()
    print(LOG_PREFIX,"DEALS_ALVO",len(deals))

    sent_count=0

    for d in deals:
        deal_id=str(d["id"])
        title=d.get("title") or ""
        labs=labels_of(d)
        phone=get_phone(d)

        origin="trafego" if LABEL_LEAD_TRAFEGO in labs else "warm"
        msgs=MSG_TRAFEGO if origin=="trafego" else MSG_WARM

        rec=state.get(deal_id, {})
        current_step=int(rec.get("step") or 0)
        next_step=current_step+1

        if next_step > 6:
            print(LOG_PREFIX,"SKIP_FINALIZADO",deal_id,title)
            continue

        if not phone:
            print(LOG_PREFIX,"SKIP_SEM_PHONE",deal_id,title)
            continue

        if not due_for_step(rec,next_step):
            print(LOG_PREFIX,"SKIP_AGUARDANDO",deal_id,"step",next_step)
            continue

        print(LOG_PREFIX,"ALVO",deal_id,"origem",origin,"step",next_step,title,"phone",phone)

        if not apply:
            continue

        if send:
            ok=send_wa(phone,msgs[next_step])
            if not ok:
                print(LOG_PREFIX,"FALHA_ENVIO",deal_id,phone)
                continue

        state[deal_id]={
            "origin":origin,
            "phone":phone,
            "step":next_step,
            "last_sent_at":now().strftime("%Y-%m-%d %H:%M:%S"),
            "title":title,
        }

        pd("POST","/notes",{
            "deal_id":int(deal_id),
            "content":f"[WA_CADENCE_{origin.upper()}_STEP_{next_step}] WhatsApp etapa {next_step}/6 {'enviada' if send else 'marcada'}."
        })

        if int(d.get("stage_id") or 0)==STAGE_PRONTO:
            pd("PUT",f"/deals/{deal_id}",{"stage_id":STAGE_TENTATIVA})

        save_state(state)
        sent_count += 1
        time.sleep(8)

        if sent_count >= limit:
            print(LOG_PREFIX,"LIMIT_ATINGIDO",limit)
            break

    save_state(state)

if __name__=="__main__":
    import sys
    main(apply="--apply" in sys.argv, send="--send" in sys.argv)
>>>>>>> 5c858a8be7eb428553fe3b537420d1f403328f7b
