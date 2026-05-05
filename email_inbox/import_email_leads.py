import os, re, imaplib, email, requests, time
from pathlib import Path
from email.header import decode_header
from dotenv import load_dotenv

load_dotenv("/root/sdr-vps/.env")

IMAP_HOST=os.getenv("LEADS_IMAP_HOST","mail.manddigital.com.br")
IMAP_USER=os.getenv("LEADS_IMAP_USER")
IMAP_PASS=os.getenv("LEADS_IMAP_PASS")
PD=os.getenv("PIPEDRIVE_API_TOKEN")
PIPELINE_ID=int(os.getenv("EMAIL_LEADS_PIPELINE_ID","7"))
STAGE_ID=int(os.getenv("EMAIL_LEADS_STAGE_ID","62"))
TAG_NAME="LEAD-TRÁFEGO"
API="https://api.pipedrive.com/v1"
PROCESSED=Path("/root/sdr-vps/data/email_inbox_processed.txt")

def hdr(v):
    if not v: return ""
    out=""
    for p,e in decode_header(v):
        out += p.decode(e or "utf-8","ignore") if isinstance(p,bytes) else p
    return out.strip()

def get_body(msg):
    parts = msg.walk() if msg.is_multipart() else [msg]
    for part in parts:
        if part.get_content_type() in ("text/plain","text/html"):
            raw=part.get_payload(decode=True) or b""
            return raw.decode(part.get_content_charset() or "utf-8","ignore")
    return ""

def clean_html(t):
    t=re.sub(r"<br\s*/?>","\n",t,flags=re.I)
    t=re.sub(r"<[^>]+>"," ",t)
    t=re.sub(r"\s+"," ",t).strip()
    return t

def field(text, names):
    for n in names:
        m=re.search(rf"{n}\s*[:\-]\s*(.+?)(?=\s+(?:Nome|Email|E-mail|Telefone|WhatsApp|Empresa|Mensagem|Data|Horário|Horario)\s*[:\-]|$)", text, re.I)
        if m: return clean_html(m.group(1))[:100]
    return ""

def clean_company_name(v):
    v=clean_html(v or "")
    v=re.split(r"\s+(?:Mensagem|Data|Horário|Horario)\s*[:\-]", v, flags=re.I)[0]
    return v.strip(" -|")[:80]

def extract(text, sender):
    txt=clean_html(text.replace("<br>","\n"))
    emails=[x for x in re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', txt+" "+sender)
            if not any(b in x.lower() for b in ["leadster","manddigital","image",".jpg",".png"])]
    phones=re.findall(r'(?:\+?55)?\s?\(?\d{2}\)?\s?\d{4,5}[-\s]?\d{4}', txt)

    nome=field(txt, ["nome","name"])
    empresa=field(txt, ["empresa","company","loja","organização","organizacao"])
    msg=field(txt, ["mensagem","message"])

    if not nome and emails:
        nome=emails[0].split("@")[0].replace("."," ").replace("_"," ").title()

    return {
        "nome": nome or "Lead LP",
        "empresa": clean_company_name(empresa),
        "email": emails[0].lower() if emails else "",
        "phone": phones[0] if phones else "",
        "mensagem": msg,
        "texto": txt[:2500],
    }

def pd(method,path,body=None,params=None):
    params=params or {}
    params["api_token"]=PD
    r=requests.request(method, API+path, params=params, json=body, timeout=30)
    r.raise_for_status()
    return r.json()

def search_person(email_addr, phone):
    term=email_addr or phone
    if not term: return None
    data=pd("GET","/persons/search",params={"term":term,"exact_match":True}).get("data",{}).get("items",[])
    return data[0]["item"] if data else None

def get_or_create_org(name):
    if not name: return None
    data=pd("GET","/organizations/search",params={"term":name,"exact_match":True}).get("data",{}).get("items",[])
    if data: return data[0]["item"]["id"]
    return pd("POST","/organizations",{"name":name})["data"]["id"]

def get_or_create_label():
    labels=pd("GET","/dealLabels").get("data") or []
    for l in labels:
        if l.get("name","").upper()==TAG_NAME.upper():
            return l["id"]
    return pd("POST","/dealLabels",{"name":TAG_NAME,"color":"green"})["data"]["id"]

def main():
    print("[EMAIL_IMPORT_START]")
    processed=set(PROCESSED.read_text().splitlines()) if PROCESSED.exists() else set()
    label_id=None

    imap=imaplib.IMAP4_SSL(IMAP_HOST,993)
    imap.login(IMAP_USER,IMAP_PASS)
    imap.select("INBOX")
    _,data=imap.search(None,"ALL")

    for mid in data[0].split()[-80:]:
        mid_s=mid.decode()
        if mid_s in processed: continue

        _,raw=imap.fetch(mid,"(RFC822)")
        msg=email.message_from_bytes(raw[0][1])
        sender=hdr(msg.get("From"))
        subject=hdr(msg.get("Subject"))
        text=get_body(msg)

        ok = ("leads@leadster.com.br" in sender.lower() and "novo lead gerado" in subject.lower()) \
             or ("comercial@manddigital.com.br" in sender.lower() and "formulário lp" in subject.lower())

        if not ok:
            processed.add(mid_s); continue

        lead=extract(text,sender)
        if not lead["email"] and not lead["phone"]:
            processed.add(mid_s); continue

        existing=search_person(lead["email"], lead["phone"])
        org_id=get_or_create_org(lead["empresa"])

        if existing:
            person_id=existing["id"]
            print("JA_EXISTE_PERSON_NOTA_APENAS", lead["email"] or lead["phone"], person_id)
            pd("POST","/notes",{
                "person_id":person_id,
                "content":f"Novo formulário recebido por email.<br>Origem: Leadster/Formulário LP<br>Assunto: {subject}<br><br>{lead['texto']}"
            })
            processed.add(mid_s)
            continue
        else:
            person_id=pd("POST","/persons",{
                "name":lead["nome"],
                "org_id":org_id,
                "email":[{"value":lead["email"],"primary":True}] if lead["email"] else [],
                "phone":[{"value":lead["phone"],"primary":True}] if lead["phone"] else []
            })["data"]["id"]

        title=f"Lead LP - {lead['empresa'] or lead['nome']}".strip()

        deal=pd("POST","/deals",{
            "title":title,
            "person_id":person_id,
            "org_id":org_id,
            "pipeline_id":PIPELINE_ID,
            "stage_id":STAGE_ID
        })["data"]

        pd("POST","/notes",{
            "deal_id":deal["id"],
            "person_id":person_id,
            "org_id":org_id,
            "content":f"Origem: Leadster/Formulário LP<br>Tag: {TAG_NAME}<br>Assunto: {subject}<br><br>{lead['texto']}"
        })

        print("OK", lead["email"] or lead["phone"], deal["id"], title)
        processed.add(mid_s)
        time.sleep(.2)

    PROCESSED.parent.mkdir(exist_ok=True)
    PROCESSED.write_text("\n".join(sorted(processed,key=int)))
    imap.logout()

if __name__=="__main__":
    main()
