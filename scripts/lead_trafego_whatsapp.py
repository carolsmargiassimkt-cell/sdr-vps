import os, re, json, time, requests, unicodedata
from pathlib import Path
from dotenv import load_dotenv

load_dotenv("/root/sdr-vps/.env")
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
API="https://api.pipedrive.com/v1"
SINCE="2026-04-20"
PIPELINE_ID=7
TARGET_STAGE_NAME="Pronto para Prospecção"
READY_STAGE_ID=63
LEAD_TRAFEGO_LABEL_ID=193
TAG_NAME="LEAD-TRÁFEGO"
FORCE_LABEL_ID=193
SENT_FILE=Path("/root/sdr-vps/data/lead_trafego_wa_sent.json")

MSG_TEMPLATE="""Oi, tudo bem? Aqui é a Carol, da Mand Digital.

Vi que você preencheu nosso formulário sobre promoções comerciais/vale-brinde e queria te chamar por aqui pra entender melhor o que vocês estão pensando em fazer.

A Mand ajuda empresas a transformar campanhas promocionais em experiências interativas, como roletas, raspadinhas e vale-brindes, capturando dados e aumentando conversão.

Faz sentido eu te explicar rapidinho por aqui?"""

def norm(s):
    s=s or ""
    s=unicodedata.normalize("NFKD",s).encode("ascii","ignore").decode()
    return re.sub(r"\s+"," ",s.lower()).strip()

def pd(method,path,json_body=None,params=None):
    params=params or {}
    params["api_token"]=TOKEN
    r=requests.request(method,API+path,params=params,json=json_body,timeout=30)
    r.raise_for_status()
    return r.json()

def phone_clean(v):
    nums=re.sub(r"\D","",v or "")
    if not nums: return ""
    if len(nums) in (10,11): nums="55"+nums
    return nums

def get_target_stage():
    data=pd("GET","/stages",params={"pipeline_id":PIPELINE_ID}).get("data") or []
    for s in data:
        if norm(s["name"])==norm(TARGET_STAGE_NAME):
            return s["id"]
    print("STAGES_DISPONIVEIS:", [(s["id"],s["name"]) for s in data])
    raise SystemExit("Stage Tentativa de Contato não encontrada")

def get_label_id():
    return FORCE_LABEL_ID
    fields=pd("GET","/dealFields").get("data") or []
    for f in fields:
        if f.get("key")=="label" or norm(f.get("name"))=="etiqueta":
            for opt in f.get("options") or []:
                if norm(opt.get("label"))==norm(TAG_NAME):
                    return opt.get("id")
    print("AVISO: label LEAD-TRÁFEGO não encontrada; vou mover e enviar sem tag.")
    return None

def get_deals():
    out=[]; start=0
    while True:
        r=pd("GET","/deals",params={"status":"open","start":start,"limit":100})
        data=r.get("data") or []
        for d in data:
            if int(d.get("pipeline_id") or 0)!=PIPELINE_ID:
                continue
            if int(d.get("stage_id") or 0)!=READY_STAGE_ID:
                continue
            if str(d.get("label") or "") != str(LEAD_TRAFEGO_LABEL_ID):
                continue
            out.append(d)
        pag=r.get("additional_data",{}).get("pagination",{})
        if not pag.get("more_items_in_collection"): break
        start += 100
    return out

def send_wa(phone,text):
    r=requests.post("http://127.0.0.1:3000/send",json={"number":phone,"text":text},timeout=45)
    print("WA",phone,r.status_code,r.text[:120])
    return r.ok

def main(apply=False, send=False):
    stage_id=get_target_stage()
    label_id=get_label_id()
    sent=json.loads(SENT_FILE.read_text()) if SENT_FILE.exists() else {}

    deals=get_deals()
    print("DEALS_ALVO",len(deals))

    seen_phones=set()
    for d in deals:
        deal_id=str(d["id"])
        person=d.get("person_id") or {}
        phone=""
        if isinstance(person,dict):
            for p in person.get("phone") or []:
                phone=phone_clean(p.get("value"))
                if phone: break

        if not phone:
            print("SKIP_SEM_PHONE",deal_id,d.get("title"))
            continue
        if phone in seen_phones:
            print("SKIP_PHONE_DUP",deal_id,d.get("title"),phone)
            continue
        seen_phones.add(phone)

        print("ALVO",deal_id,d.get("title"),"phone=",phone)

        if not apply: continue

        body={"stage_id":stage_id}
        if label_id: body["label"]=label_id
        pd("PUT",f"/deals/{deal_id}",body)

        if send and phone and deal_id not in sent:
            ok=send_wa(phone,MSG_TEMPLATE)
            if ok:
                sent[deal_id]={"phone":phone,"sent_at":time.strftime("%Y-%m-%d %H:%M:%S")}
                pd("POST","/notes",{"deal_id":int(deal_id),"content":"[LEAD_TRAFEGO_WA1_ENVIADO] Abertura WhatsApp enviada para lead inbound/tráfego."})
                time.sleep(8)

    SENT_FILE.parent.mkdir(exist_ok=True)
    SENT_FILE.write_text(json.dumps(sent,ensure_ascii=False,indent=2))

if __name__=="__main__":
    import sys
    main(apply="--apply" in sys.argv, send="--send" in sys.argv)
