import imaplib, email, os, json, requests, re
from pathlib import Path
from email.header import decode_header

IMAP_HOST=os.getenv("SMTP_HOST")
IMAP_USER=os.getenv("SMTP_USER")
IMAP_PASS=os.getenv("SMTP_PASS")
TOKEN=os.getenv("PIPEDRIVE_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"
QUEUE=Path("/root/sdr-vps/data/email_cadence_queue.json")

RESPONDIDO_LABEL_ID=196

def clean_sender(raw):
    m=re.search(r'<([^>]+)>', raw or "")
    return (m.group(1) if m else raw or "").strip().lower()

def decode_subject(s):
    if not s: return ""
    parts=decode_header(s)
    out=""
    for txt, enc in parts:
        if isinstance(txt, bytes):
            out += txt.decode(enc or "utf-8", errors="ignore")
        else:
            out += txt
    return out

def api(method,path,**kw):
    params=kw.pop("params",{})
    params["api_token"]=TOKEN
    r=requests.request(method, BASE+path, params=params, timeout=20, **kw)
    if r.status_code>=400:
        print("[PIPEDRIVE_FAIL]", method, path, r.status_code, r.text[:200])
        return None
    return r.json()

def find_deal_by_email(addr):
    q=json.load(open(QUEUE,encoding="utf-8")) if QUEUE.exists() else []
    for x in q:
        if (x.get("email") or "").lower()==addr:
            return int(x["deal_id"])
    return None

def stop_cadence(addr, deal_id):
    if not QUEUE.exists(): return
    q=json.load(open(QUEUE,encoding="utf-8"))
    changed=0
    for x in q:
        if int(x.get("deal_id",0))==deal_id or (x.get("email") or "").lower()==addr:
            if x.get("status") not in ("done","stopped"):
                x["status"]="stopped"
                x["stop_reason"]="email_replied"
                changed+=1
    json.dump(q, open(QUEUE,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    print("[CADENCE_STOPPED]", deal_id, addr, changed)

def mark_crm(deal_id, sender, subject):
    note=f"<b>Resposta recebida por email</b><br><b>De:</b> {sender}<br><b>Assunto:</b> {subject}<br><b>Ação:</b> cadência parada automaticamente."
    api("POST","/notes",json={"deal_id":deal_id,"content":note})
    api("PUT",f"/deals/{deal_id}",json={"label":RESPONDIDO_LABEL_ID})
    print("[CRM_RESPONDIDO]", deal_id)

def run():
    mail=imaplib.IMAP4_SSL(IMAP_HOST)
    mail.login(IMAP_USER, IMAP_PASS)
    mail.select("INBOX")
    typ,data=mail.search(None,'UNSEEN')
    for num in data[0].split():
        typ,msg_data=mail.fetch(num,'(RFC822)')
        msg=email.message_from_bytes(msg_data[0][1])
        sender=clean_sender(msg.get("From",""))
        subject=decode_subject(msg.get("Subject",""))

        # ignora bounces/postmaster
        if "postmaster" in sender or "mailer-daemon" in sender:
            print("[BOUNCE_IGNORED]", sender, subject)
            mail.store(num,'+FLAGS','\\Seen')
            continue

        deal_id=find_deal_by_email(sender)
        print("[EMAIL_REPLY]", sender, subject, "deal=", deal_id)

        if deal_id:
            mark_crm(deal_id, sender, subject)
            stop_cadence(sender, deal_id)

        mail.store(num,'+FLAGS','\\Seen')
    mail.logout()

if __name__=="__main__":
    run()
