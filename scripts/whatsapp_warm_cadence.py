import os, re, json, time, requests
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from core.sdr_state import STAGE_TENTATIVA_CONTATO, log_event, update_deal_state

load_dotenv("/root/sdr-vps/.env")

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
API="https://api.pipedrive.com/v1"

PIPELINE_ID=7
STAGE_PRONTO=63
STAGE_TENTATIVA=STAGE_TENTATIVA_CONTATO

LABEL_LEAD_TRAFEGO="193"
LABEL_WARM_WHATSAPP="226"
LABEL_RESPONDIDO="196"

STATE_FILE=Path("/root/sdr-vps/data/whatsapp_warm_cadence.json")
BLOCKLIST_FILES=[
    Path("/root/sdr-vps/data/whatsapp_manual_blocklist.json"),
    Path("/root/sdr-vps/data/whatsapp_blocklist.json"),
    Path("/root/sdr-vps/logs/whatsapp_manual_blocklist.json"),
    Path("/root/sdr-vps/invalidos.json"),
]
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


def is_brazil_business_hours():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    try:
        import holidays
        br_holidays = holidays.Brazil()
    except Exception:
        br_holidays = set()

    now = datetime.now(ZoneInfo("America/Sao_Paulo"))
    if now.weekday() >= 5:
        return False
    if now.date() in br_holidays:
        return False
    return 9 <= now.hour < 18


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

def send_wa(phone,text):
    r=requests.post("http://127.0.0.1:3000/send",json={"number":phone,"text":text},timeout=60)
    print(LOG_PREFIX,"WA",phone,r.status_code,r.text[:160])
    return r.ok and '"sent"' in r.text

def main(apply=False, send=False, limit=10):
    if not is_brazil_business_hours():
        print(LOG_PREFIX, "FORA_HORARIO_COMERCIAL_BR")
        return

    state=load_state()
    deals=get_open_deals()
    blocked=load_blocklist()
    stopped_index=state.get("stopped") if isinstance(state.get("stopped"), dict) else {}
    print(LOG_PREFIX,"DEALS_ALVO",len(deals))

    sent_count=0

    for d in select_batch_deals(deals, blocked):
        deal_id=str(d["id"])
        title=d.get("title") or ""
        phone=d.get("_phone") or get_phone(d)

        if d.get("_skip_reason")=="blocklist":
            print(LOG_PREFIX,"SKIP_BLOCKLIST",deal_id,phone)
            log_event("WA_SKIP_BLOCKLIST", deal_id=deal_id, phone=phone)
            continue
        if d.get("_skip_reason")=="dup_batch":
            print(LOG_PREFIX,"SKIP_PHONE_DUP_BATCH",deal_id,phone)
            log_event("WA_SKIP_DUP", deal_id=deal_id, phone=phone, reason="phone_dup_batch")
            continue

        if not phone:
            print(LOG_PREFIX,"SKIP_SEM_PHONE",deal_id,title)
            continue

        rec=state.get(deal_id, {})
        if deal_id in stopped_index or f"phone:{phone}" in stopped_index:
            reason=(stopped_index.get(deal_id) or stopped_index.get(f"phone:{phone}") or {}).get("reason")
            print(LOG_PREFIX,"WA_STOPPED",deal_id,phone,reason or "stopped_index")
            log_event("WA_STOPPED", deal_id=deal_id, phone=phone, reason=reason or "stopped_index")
            continue
        if rec.get("stopped"):
            print(LOG_PREFIX,"WA_STOPPED",deal_id,phone)
            log_event("WA_STOPPED", deal_id=deal_id, phone=phone, reason="state_stopped")
            continue
        current_step=int(rec.get("step") or 0)
        next_step=current_step+1

        labs=labels_of(d)
        origin="trafego" if LABEL_LEAD_TRAFEGO in labs else "warm"
        msgs=MSG_TRAFEGO if origin=="trafego" else MSG_WARM

        if next_step > 6:
            print(LOG_PREFIX,"SKIP_FINALIZADO",deal_id,title)
            continue

        if not due_for_step(rec,next_step):
            print(LOG_PREFIX,"SKIP_AGUARDANDO",deal_id,"step",next_step)
            continue

        print(LOG_PREFIX,"ALVO",deal_id,"origem",origin,"step",next_step,title,"phone",phone)
        log_event("WA_TARGET", deal_id=deal_id, phone=phone, origin=origin, step=next_step)

        if not apply:
            continue

        if not send:
            print(LOG_PREFIX,"DRY_RUN_NO_MUTATION",deal_id,phone,"step",next_step)
            continue

        ok=send_wa(phone,msgs[next_step])
        if not ok:
            print(LOG_PREFIX,"FALHA_ENVIO",deal_id,phone)
            continue

        state[deal_id]={
            "origin":origin,
            "phone":phone,
            "step":next_step,
            "wa1_sent": bool(next_step == 1 or rec.get("wa1_sent")),
            "last_sent_step_whatsapp": next_step,
            "last_sent_at":now().strftime("%Y-%m-%d %H:%M:%S"),
            "title":title,
        }
        update_deal_state(
            deal_id,
            phone=phone,
            origin=origin,
            cadence_wa_active=next_step < 6,
            last_sender="whatsapp_warm_cadence",
            last_sent_step_whatsapp=next_step,
            last_outbound_at=now().isoformat(timespec="seconds"),
        )

        pd("POST","/notes",{
            "deal_id":int(deal_id),
            "content":f"[WA_CADENCE_{origin.upper()}_STEP_{next_step}] WhatsApp etapa {next_step}/6 enviada."
        })
        log_event("WA_SENT", deal_id=deal_id, phone=phone, origin=origin, step=next_step)

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
