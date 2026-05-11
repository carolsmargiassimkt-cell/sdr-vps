from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.stage_router import resolve_pipeline_stage
import os, re, json, time, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from datetime import timezone
import hashlib, random
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False
from core.sdr_orchestrator import ACTION_WA_WARM_SEND, load_email_state, resolve_next_action
from core.sdr_state import STAGE_TENTATIVA_CONTATO, log_event, update_deal_state
from core.wa_strategy_state import load_strategy_state, mark_warm, save_strategy_state
from core.auto_reply_guard import is_auto_reply_blocked
from core.human_handoff import is_human_handoff
from core.locked_json_state import locked_load_json, locked_save_json
from core.inflight_actions import acquire_inflight, release_inflight
from core.pipedrive_safe import safe_pd_request

load_dotenv("/root/sdr-vps/.env")

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
API="https://api.pipedrive.com/v1"

PIPELINE_ID=7
STAGE_PRONTO=52
STAGE_TENTATIVA=STAGE_TENTATIVA_CONTATO

LABEL_LEAD_TRAFEGO="193"
LABEL_WARM_WHATSAPP="226"
LABEL_RESPONDIDO="196"

STATE_FILE=Path("/root/sdr-vps/data/whatsapp_warm_cadence.json")
MEETING_STATE_FILE=Path("/root/sdr-vps/data/sdr_meeting_reminders.json")
BLOCKLIST_FILES=[
    Path("/root/sdr-vps/data/whatsapp_manual_blocklist.json"),
    Path("/root/sdr-vps/data/whatsapp_blocklist.json"),
    Path("/root/sdr-vps/logs/whatsapp_manual_blocklist.json"),
    Path("/root/sdr-vps/invalidos.json"),
]
LOG_PREFIX="[WA_WARM_CADENCE]"

MAX_WARM_STEP=4

def br_tz():
    try:
        return ZoneInfo("America/Sao_Paulo")
    except Exception:
        return timezone(timedelta(hours=-3))

INTERVALS_DAYS={
    1:0,
    2:1,
    3:2,
    4:3,
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

WARM_TEMPLATES={
    "trafego":{
        1:[
            "{Oi|Olá}, tudo bem? Aqui é a Carol, da Mand Digital.\n\nVi seu cadastro pelo {formulário|site|Leadster} e imaginei que vocês estejam olhando alguma ação promocional.\n\nA Mand cria campanhas interativas com QR Code, roleta, raspadinha e captura de dados próprios. {Posso te mandar um exemplo curto?|Te mando uma ideia rápida aplicada à Copa?}",
            "{Oi|Olá}! Carol da Mand Digital por aqui.\n\nRecebi seu interesse pelo formulário e queria ir direto ao ponto: ajudamos marcas a transformar campanha de Copa/PDV em participação, lead e remarketing.\n\n{Posso te mostrar um exemplo?|Quer ver uma ideia prática?}",
            "Oi, tudo bem? Sou a Carol, da Mand Digital.\n\nVi seu contato chegando por aqui. Para {varejo|shopping|supermercado}, a gente costuma usar QR Code + mecânica interativa para gerar dados first-party e conversão.\n\n{Posso te mandar um exemplo rápido?|Te envio uma ideia simples?}",
        ],
        2:[
            "Passando com um exemplo direto: campanha de Copa no PDV com QR Code, roleta digital e prêmio simples. O cliente participa, deixa nome/WhatsApp e vocês conseguem medir loja, canal e recompra.\n\n{Esse tipo de ação conversa com o que vocês buscam?|Faz sentido para o momento de vocês?}",
            "Uma aplicação bem prática: raspadinha digital ou vale-brinde para quem compra, com captura de dados e régua de WhatsApp depois. Não é só promoção; vira base própria para novas campanhas.\n\n{Quer que eu adapte um exemplo para vocês?|Posso montar uma ideia enxuta?}",
            "Para campanhas de Copa, o que mais funciona é deixar a mecânica fácil: viu o QR Code, participou, ganhou chance/prêmio e entrou na base. Depois dá para segmentar e fazer remarketing.\n\n{Quer ver como ficaria no seu contexto?|Te mando um modelo curto?}",
        ],
        3:[
            "Para eu não te mandar algo genérico: vocês querem mais {captar contatos|aumentar fluxo no PDV|vender mais em uma data específica|medir resultado da campanha}?",
            "Me diz só uma coisa: a prioridade hoje é {trazer mais gente para loja|ativar clientes antigos|capturar dados próprios|criar uma ação de Copa}?",
            "Se fizer sentido avançar, eu te mando uma ideia já no formato de campanha. O foco seria {Copa|PDV|varejo|captação de dados|conversão}?",
        ],
        4:[
            "Tudo bem, vou encerrar por aqui para não insistir.\n\nSe quiser retomar uma campanha interativa de Copa, QR Code, roleta ou captura de dados, é só me chamar.",
            "Sem problema. Vou parar por aqui.\n\nQuando fizer sentido olhar campanha promocional com dados próprios e mensuração, fico à disposição.",
            "Combinado, não vou insistir.\n\nSe em algum momento vocês quiserem transformar campanha de PDV/Copa em participação e base própria, pode me chamar por aqui.",
        ],
    },
    "warm":{
        1:[
            "{Oi|Olá}, tudo bem? Aqui é a Carol, da Mand Digital.\n\nVi sua interação com nosso material e achei que valia te chamar por aqui. A gente cria campanhas interativas para varejo, shopping e supermercado com QR Code, roleta/raspadinha e captura de dados.\n\n{Quer que eu te mande um exemplo rápido?|Te mando uma ideia curta de campanha?}",
            "Oi! Carol da Mand Digital por aqui.\n\nComo você clicou/interagiu com nosso conteúdo, vou ser objetiva: a Mand ajuda campanhas promocionais a virarem participação, dados first-party e remarketing.\n\n{Quer ver um exemplo prático?|Posso te mandar uma ideia de Copa?}",
            "{Olá|Oi}, tudo certo? Sou a Carol, da Mand.\n\nVi seu interesse e pensei em uma aplicação simples: campanha de Copa ou PDV com QR Code, benefício instantâneo e mensuração de conversão.\n\n{Te mando um exemplo curto?|Quer ver como funciona na prática?}",
        ],
        2:[
            "Exemplo rápido: no supermercado ou shopping, o cliente acessa um QR Code, participa de uma roleta/raspadinha, deixa contato e recebe uma oferta. A empresa ganha engajamento e base própria para próximas ações.\n\n{Faz sentido para vocês?|Quer que eu adapte para o seu segmento?}",
            "O ponto forte não é só a mecânica bonita. É conseguir saber quem participou, de onde veio e como acionar depois por WhatsApp ou mídia própria.\n\n{Quer que eu te mostre um modelo simples?|Posso te mandar uma ideia aplicada?}",
            "Para Copa, uma ação enxuta pode juntar PDV + QR Code + premiação instantânea + dados first-party. Isso ajuda a vender no período e ainda deixa uma base para depois.\n\n{Quer que eu desenhe um exemplo rápido?|Isso conversa com o que vocês estão buscando?}",
        ],
        3:[
            "Se fizer sentido, o próximo passo pode ser simples: eu entendo o segmento e te mando uma ideia de campanha já adaptada.\n\n{Você prefere exemplo por aqui ou uma conversa rápida?|Quer que eu adapte para vocês?}",
            "Para avançar sem enrolar: vocês estão pensando em algo para {Copa|Dia dos Namorados|férias|PDV|captação de leads} ou é mais benchmarking agora?",
            "Me fala só o melhor caminho: {te mando um exemplo objetivo por aqui|marcamos 15 min para eu entender o cenário|deixo para outro momento}?",
        ],
        4:[
            "Vou encerrar por aqui para não ficar repetindo mensagem.\n\nSe quiser retomar a ideia de campanha interativa com QR Code, Copa, roleta ou captura de dados, fico à disposição.",
            "Sem problema, paro por aqui.\n\nQuando fizer sentido olhar uma ação promocional mais mensurável e com dados próprios, pode me chamar.",
            "Combinado. Não vou insistir.\n\nSe a pauta de campanha interativa voltar, me chama que eu te mando um exemplo direto.",
        ],
    },
}

MEETING_REMINDER_TEMPLATES={
    "d_minus_1":[
        "Oi, tudo bem? Passando só para confirmar nossa reunião de amanhã às {time}.",
        "Olá! Tudo certo para nossa conversa amanhã às {time}?",
        "Oi! Só confirmando nossa reunião de amanhã às {time}. Te espero por lá.",
    ],
    "d0":[
        "Oi, tudo bem? Confirmando nossa reunião de hoje às {time}.",
        "Olá! Passando para lembrar da nossa conversa hoje às {time}.",
        "Oi! Nossa reunião é hoje às {time}. Tudo certo por aí?",
    ],
}


def is_brazil_business_hours():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    try:
        import holidays
        br_holidays = holidays.Brazil()
    except Exception:
        br_holidays = set()

    now = datetime.now(br_tz())
    if now.weekday() >= 5:
        return False
    if now.date() in br_holidays:
        return False
    return 9 <= now.hour < 18


def now():
    return datetime.now(br_tz()).replace(tzinfo=None)

def today_key():
    return datetime.now(br_tz()).date().isoformat()

def load_state():
    payload = locked_load_json(STATE_FILE, {})
    return payload if isinstance(payload, dict) else {}

def save_state(state):
    locked_save_json(STATE_FILE, state if isinstance(state, dict) else {})

def load_meeting_state():
    payload = locked_load_json(MEETING_STATE_FILE, {})
    return payload if isinstance(payload, dict) else {}

def pd(method,path,json_body=None,params=None):
    return safe_pd_request(method, path, json_body, params) or {}

def phone_clean(v):
    nums=re.sub(r"\D","",v or "")
    if len(nums) in (10,11):
        nums="55"+nums
    return nums

def phone_variants(v):
    num=phone_clean(v)
    variants=set()
    if not num:
        return variants
    variants.add(num)
    if num.startswith("55"):
        variants.add(num[2:])
    elif len(num) in (10,11):
        variants.add("55"+num)
    return {x for x in variants if x}

def iter_blocklist_values(payload):
    if isinstance(payload, dict):
        for key, value in payload.items():
            yield key
            if isinstance(value, dict):
                for field in ("phone","telefone","number","numero","value"):
                    yield value.get(field)
            elif isinstance(value, (str, int)):
                yield value
            elif isinstance(value, list):
                for item in value:
                    yield item
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                for field in ("phone","telefone","number","numero","value"):
                    yield item.get(field)
            else:
                yield item

def load_blocklist():
    blocked=set()
    for path in BLOCKLIST_FILES:
        if not path.exists():
            continue
        try:
            payload=json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            print(LOG_PREFIX,"BLOCKLIST_READ_FAIL",str(path),str(exc))
            continue
        for raw in iter_blocklist_values(payload):
            blocked.update(phone_variants(raw))
    return blocked

def is_blocked_phone(phone, blocked):
    return bool(phone_variants(phone) & blocked)

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
        try:
            r=pd("GET","/deals",params={"status":"open","start":start,"limit":100,"sort":"add_time DESC"})
        except Exception as exc:
            print(LOG_PREFIX,"PIPEDRIVE_READ_FAIL",str(exc).split(" for url: ")[0])
            return out
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

def select_batch_deals(deals, blocked):
    selected=[]
    seen_phone={}
    enriched=[]
    for d in deals:
        deal_id=int(d.get("id") or 0)
        phone=get_phone(d)
        title=d.get("title") or ""
        if not phone:
            item=dict(d)
            item["_phone"]=""
            item["_skip_reason"]="no_phone"
            enriched.append(item)
            continue
        if is_blocked_phone(phone, blocked):
            item=dict(d)
            item["_phone"]=phone
            item["_skip_reason"]="blocklist"
            enriched.append(item)
            continue
        item=dict(d)
        item["_phone"]=phone
        item["_skip_reason"]=""
        enriched.append(item)
        previous=seen_phone.get(phone)
        if previous is None or deal_id > int(previous.get("id") or 0):
            seen_phone[phone]=item
    winners={int(item.get("id") or 0) for item in seen_phone.values()}
    for item in enriched:
        if item.get("_skip_reason"):
            selected.append(item)
            continue
        if int(item.get("id") or 0) not in winners:
            item["_skip_reason"]="dup_batch"
        selected.append(item)
    return selected

def due_for_step(record, next_step):
    if next_step == 1:
        return True
    last=record.get("last_sent_at")
    if not last:
        return True
    last_dt=datetime.strptime(last,"%Y-%m-%d %H:%M:%S")
    return now() >= last_dt + timedelta(days=INTERVALS_DAYS.get(next_step, 99))

def sent_today(record):
    raw=str((record or {}).get("last_sent_at") or "").strip()
    return bool(raw and raw[:10] == today_key())

def already_sent_today(state, deal_id, phone):
    rec=state.get(str(deal_id), {}) if isinstance(state, dict) else {}
    if sent_today(rec):
        return True, rec.get("last_sent_at") or "", "deal_sent_today"
    clean=phone_clean(phone)
    for key, item in (state or {}).items():
        if not isinstance(item, dict):
            continue
        if phone_clean(item.get("phone")) == clean and sent_today(item):
            return True, item.get("last_sent_at") or "", f"phone_sent_today:{key}"
    return False, "", ""

def _rng_for(deal_id, phone, origin, step):
    seed=f"{deal_id}:{phone_clean(phone)}:{origin}:{step}:{today_key()}"
    digest=hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return random.Random(int(digest[:16], 16))

def render_spintax(text, rng=None):
    rng=rng or random.Random()
    rendered=str(text or "")
    pattern=re.compile(r"\{([^{}|]+(?:\|[^{}|]+)+)\}")
    for _ in range(20):
        match=pattern.search(rendered)
        if not match:
            break
        options=[part.strip() for part in match.group(1).split("|") if part.strip()]
        rendered=rendered[:match.start()] + (rng.choice(options) if options else "") + rendered[match.end():]
    return rendered.replace("{","").replace("}","").replace("|","").strip()

def render_warm_message(deal, origin, step, phone):
    templates=WARM_TEMPLATES.get(origin) or WARM_TEMPLATES["warm"]
    options=templates.get(int(step)) or []
    rng=_rng_for((deal or {}).get("id") or 0, phone, origin, step)
    raw=rng.choice(options) if options else ""
    msg=render_spintax(raw, rng)
    msg=re.sub(r"\n{3,}", "\n\n", msg).strip()
    print("[WA_WARM_MSG_SELECT]", "deal_id=", (deal or {}).get("id"), "phone=", phone, "origin=", origin, "step=", step, "template_count=", len(options))
    if any(token in msg for token in ("{","}","|")):
        raise ValueError("spintax_not_rendered")
    return msg

def send_wa(phone,text):
    if any(token in str(text or "") for token in ("{","}","|")):
        print(LOG_PREFIX,"SPINTAX_NOT_RENDERED_BLOCK",phone)
        return False
    r=requests.post("http://127.0.0.1:3000/send",json={"number":phone,"text":text},timeout=60)
    print(LOG_PREFIX,"WA",phone,r.status_code,r.text[:160])
    return r.ok and '"sent"' in r.text

def deal_has_meeting_booked(deal_id, phone, meeting_state=None):
    state=meeting_state if isinstance(meeting_state, dict) else load_meeting_state()
    rec=state.get(str(deal_id)) if isinstance(state, dict) else {}
    if isinstance(rec, dict) and str(rec.get("status") or "").lower() == "booked":
        return True, "meeting_state"
    try:
        data=pd("GET","/activities",params={"deal_id":int(deal_id),"done":0,"limit":100}).get("data") or []
    except Exception:
        data=[]
    today=today_key()
    for item in data:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").strip().lower() != "meeting":
            continue
        due=str(item.get("due_date") or "").strip()
        if due and due >= today:
            return True, "future_meeting_activity"
    return False, ""

def render_meeting_reminder(record, reminder_key):
    options=MEETING_REMINDER_TEMPLATES.get(str(reminder_key or "d0"), MEETING_REMINDER_TEMPLATES["d0"])
    seed=f"meeting:{record.get('deal_id')}:{record.get('phone')}:{reminder_key}:{today_key()}"
    rng=random.Random(int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16],16))
    return rng.choice(options).replace("{time}", str(record.get("meeting_time") or "o horário combinado")).strip()

def process_meeting_reminders(meeting_state, *, apply=False, send=False, limit=10):
    sent=0
    today=today_key()
    for key, rec in list((meeting_state or {}).items()):
        if not isinstance(rec, dict):
            continue
        deal_id=str(rec.get("deal_id") or key)
        phone=phone_clean(rec.get("phone"))
        status=str(rec.get("status") or "").strip().lower()
        meeting_date=str(rec.get("meeting_date") or "").strip()
        reminders=rec.get("reminders") if isinstance(rec.get("reminders"), dict) else {}
        if status == "cancelled":
            print("[MEETING_REMINDER_SKIP_CANCELLED]", "deal_id=", deal_id)
            continue
        if status == "confirmed":
            print("[MEETING_REMINDER_SKIP_CONFIRMED]", "deal_id=", deal_id)
            continue
        if meeting_date and meeting_date < today:
            print("[MEETING_REMINDER_SKIP_PAST]", "deal_id=", deal_id, "meeting_date=", meeting_date)
            continue
        if not phone:
            continue
        due_keys=[]
        try:
            meeting_dt=datetime.strptime(meeting_date,"%Y-%m-%d").date()
            current_dt=datetime.strptime(today,"%Y-%m-%d").date()
            if current_dt == meeting_dt - timedelta(days=1):
                due_keys.append("d_minus_1")
            if current_dt == meeting_dt:
                due_keys.append("d0")
        except Exception:
            continue
        for reminder_key in due_keys:
            if reminders.get(reminder_key) in {"sent","confirmed","cancelled"}:
                print("[MEETING_REMINDER_SKIP_CONFIRMED]", "deal_id=", deal_id, "reminder=", reminder_key)
                continue
            msg=render_meeting_reminder(rec, reminder_key)
            print("[MEETING_REMINDER_QUEUE]", "deal_id=", deal_id, "phone=", phone, "reminder=", reminder_key)
            if not apply:
                continue
            if not send:
                print(LOG_PREFIX,"DRY_RUN_NO_MUTATION",deal_id,phone,"meeting_reminder",reminder_key)
                continue
            ok=send_wa(phone,msg)
            if not ok:
                continue
            reminders[reminder_key]="sent"
            rec["reminders"]=reminders
            rec["last_reminder_sent_at"]=now().strftime("%Y-%m-%d %H:%M:%S")
            meeting_state[str(key)]=rec
            print("[MEETING_REMINDER_SENT]", "deal_id=", deal_id, "phone=", phone, "reminder=", reminder_key)
            sent += 1
            if sent >= limit:
                return sent
    return sent

def main(apply=False, send=False, limit=10):
    if not is_brazil_business_hours():
        print(LOG_PREFIX, "FORA_HORARIO_COMERCIAL_BR")
        return

    state=load_state()
    meeting_state=load_meeting_state()
    strategy_state=load_strategy_state()
    email_state=load_email_state()
    strategy_changed=False
    deals=get_open_deals()
    blocked=load_blocklist()
    stopped_index=state.get("stopped") if isinstance(state.get("stopped"), dict) else {}
    print(LOG_PREFIX,"DEALS_ALVO",len(deals))

    sent_count=0
    state_changed=False
    meeting_sent=process_meeting_reminders(meeting_state, apply=apply, send=send, limit=limit)
    if apply and send and meeting_sent:
        locked_save_json(MEETING_STATE_FILE, meeting_state)

    for d in select_batch_deals(deals, blocked):
        deal_id=str(d["id"])
        title=d.get("title") or ""
        phone=d.get("_phone") or get_phone(d)

        if d.get("_skip_reason")=="blocklist":
            print(LOG_PREFIX,"SKIP_BLOCKLIST",deal_id,phone)
            if apply:
                log_event("WA_SKIP_BLOCKLIST", deal_id=deal_id, phone=phone)
            continue
        if is_auto_reply_blocked(phone):
            print("[AUTO_REPLY_SKIP_SEND]", "deal_id=", deal_id, "phone=", phone, "channel=wa_warm")
            if apply:
                log_event("WA_STOPPED", deal_id=deal_id, phone=phone, reason="auto_reply_detected")
            continue
        if d.get("_skip_reason")=="dup_batch":
            print(LOG_PREFIX,"SKIP_PHONE_DUP_BATCH",deal_id,phone)
            if apply:
                log_event("WA_SKIP_DUP", deal_id=deal_id, phone=phone, reason="phone_dup_batch")
            continue

        if not phone:
            print(LOG_PREFIX,"SKIP_SEM_PHONE",deal_id,title)
            continue
        if is_human_handoff(phone):
            print("[WA_WARM_SKIP_HUMAN_HANDOFF]", "deal_id=", deal_id, "phone=", phone)
            if apply:
                log_event("WA_STOPPED", deal_id=deal_id, phone=phone, reason="human_handoff")
            continue

        rec=state.get(deal_id, {})
        if deal_id in stopped_index or f"phone:{phone}" in stopped_index:
            reason=(stopped_index.get(deal_id) or stopped_index.get(f"phone:{phone}") or {}).get("reason")
            print(LOG_PREFIX,"WA_STOPPED",deal_id,phone,reason or "stopped_index")
            if apply:
                log_event("WA_STOPPED", deal_id=deal_id, phone=phone, reason=reason or "stopped_index")
            continue
        if rec.get("stopped"):
            print(LOG_PREFIX,"WA_STOPPED",deal_id,phone)
            if apply:
                log_event("WA_STOPPED", deal_id=deal_id, phone=phone, reason="state_stopped")
            continue
        current_step=int(rec.get("step") or 0)
        next_step=current_step+1

        labs=labels_of(d)
        origin="trafego" if LABEL_LEAD_TRAFEGO in labs else "warm"

        if next_step > MAX_WARM_STEP:
            print(LOG_PREFIX,"SKIP_FINALIZADO",deal_id,title)
            print("[WA_WARM_CADENCE_DONE]", "deal_id=", deal_id, "phone=", phone, "step=", current_step)
            if apply and not rec.get("stopped"):
                rec["stopped"]=True
                rec["stop_reason"]="max_warm_step"
                rec["stopped_at"]=now().strftime("%Y-%m-%d %H:%M:%S")
                state[deal_id]=rec
                state_changed=True
                print("[WA_WARM_FORCE_STOP]", "deal_id=", deal_id, "phone=", phone, "step=", current_step)
            continue

        has_meeting, meeting_reason = deal_has_meeting_booked(deal_id, phone, meeting_state)
        if has_meeting:
            print("[WA_WARM_SKIP_MEETING_BOOKED]", "deal_id=", deal_id, "phone=", phone, "reason=", meeting_reason)
            continue

        blocked_today, last_sent_at, today_reason = already_sent_today(state, deal_id, phone)
        if blocked_today:
            print("[WA_WARM_SKIP_ALREADY_SENT_TODAY]", "deal_id=", deal_id, "phone=", phone, "last_sent_at=", last_sent_at, "reason=", today_reason)
            continue

        if not due_for_step(rec,next_step):
            print(LOG_PREFIX,"SKIP_AGUARDANDO",deal_id,"step",next_step)
            continue

        # Segurança: não avançar para step 2+ sem resposta real do lead.
        # Lead frio/warm de LP recebe abertura e depois aguarda interação humana/inbound.
        if next_step > 1 and not (
            rec.get("lead_replied")
            or rec.get("replied")
            or rec.get("last_inbound_at")
            or rec.get("last_reply_at")
            or rec.get("responded_at")
        ):
            print(LOG_PREFIX, "SKIP_SEM_RESPOSTA_LEAD", deal_id, "step", next_step)
            continue

        msg=render_warm_message(d, origin, next_step, phone)
        print(LOG_PREFIX,"ALVO",deal_id,"origem",origin,"step",next_step,title,"phone",phone)
        if apply:
            log_event("WA_TARGET", deal_id=deal_id, phone=phone, origin=origin, step=next_step)

        decision = resolve_next_action(
            d,
            email_state=email_state,
            wa_state=strategy_state,
            channel="wa_warm",
            phone=phone,
        )
        if decision.get("action") != ACTION_WA_WARM_SEND:
            print("[ORCH_BLOCK] channel=wa_warm deal_id=", deal_id, "phone=", phone, "reason=", decision.get("reason"))
            continue
        if apply:
            log_event("WA_WARM_TRIGGER", deal_id=deal_id, phone=phone, origin=origin, step=next_step)

        if not apply:
            continue

        if not send:
            print(LOG_PREFIX,"DRY_RUN_NO_MUTATION",deal_id,phone,"step",next_step)
            continue

        inflight_ok, inflight_reason = acquire_inflight(deal_id, "whatsapp")
        if not inflight_ok:
            print("[INFLIGHT_BLOCK]", "deal_id=", deal_id, "channel=whatsapp", "reason=", inflight_reason)
            continue
        try:
            ok=send_wa(phone,msg)
            if not ok:
                print(LOG_PREFIX,"FALHA_ENVIO",deal_id,phone)
                continue
        finally:
            release_inflight(deal_id, "whatsapp")

        new_rec=dict(rec)
        new_rec.update({
            "origin":origin,
            "phone":phone,
            "step":next_step,
            "wa1_sent": bool(next_step == 1 or rec.get("wa1_sent")),
            "last_sent_step_whatsapp": next_step,
            "last_sent_at":now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_message_preview": msg[:180],
            "stopped": bool(next_step >= MAX_WARM_STEP),
            "title":title,
        })
        state[deal_id]=new_rec
        state_changed=True
        print("[WA_WARM_STEP_ADVANCE]", "deal_id=", deal_id, "phone=", phone, "from_step=", current_step, "to_step=", next_step)
        if next_step >= MAX_WARM_STEP:
            print("[WA_WARM_CADENCE_DONE]", "deal_id=", deal_id, "phone=", phone, "step=", next_step)
        update_deal_state(
            deal_id,
            phone=phone,
            origin=origin,
            cadence_wa_active=next_step < MAX_WARM_STEP,
            last_sender="whatsapp_warm_cadence",
            last_sent_step_whatsapp=next_step,
            last_outbound_at=now().isoformat(timespec="seconds"),
        )

        pd("POST","/notes",{
            "deal_id":int(deal_id),
            "content":f"[WA_CADENCE_{origin.upper()}_STEP_{next_step}] WhatsApp etapa {next_step}/{MAX_WARM_STEP} enviada."
        })
        log_event("WA_SENT", deal_id=deal_id, phone=phone, origin=origin, step=next_step)
        mark_warm(strategy_state, deal_id, phone, reason=f"{origin}_sent_step_{next_step}")
        strategy_changed=True
        log_event("WA_WARM_SENT", deal_id=deal_id, phone=phone, origin=origin, step=next_step)

        if int(d.get("stage_id") or 0)==STAGE_PRONTO:
            pd("PUT",f"/deals/{deal_id}",{"stage_id":STAGE_TENTATIVA})
            print("[ORCH_STAGE_MOVE] deal_id=", deal_id, "stage_id=", STAGE_TENTATIVA)

        save_state(state)
        state_changed=False
        if strategy_changed:
            save_strategy_state(strategy_state)
        sent_count += 1
        time.sleep(8)

        if sent_count >= limit:
            print(LOG_PREFIX,"LIMIT_ATINGIDO",limit)
            break

    if apply and state_changed:
        save_state(state)
    if strategy_changed:
        save_strategy_state(strategy_state)

if __name__=="__main__":
    import sys
    main(apply="--apply" in sys.argv, send="--send" in sys.argv)


def route_warm_stage_safe(deal_id=None, source="warm_whatsapp", intent=None):
    try:
        route = resolve_pipeline_stage({
            "source": source,
            "intent": intent,
            "has_phone": True,
        })
        print("[STAGE_ROUTER_WARM]", deal_id, route)
        return route
    except Exception as exc:
        print("[STAGE_ROUTER_WARM_FAIL]", deal_id, exc)
        return {"action":"skip","reason":"error"}
