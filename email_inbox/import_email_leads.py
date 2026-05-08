import os, re, imaplib, email, requests, time, json, sys
from pathlib import Path
from email.header import decode_header
from dotenv import load_dotenv

# Load environment variables from multiple possible locations
load_dotenv(Path(".") / ".env")
load_dotenv("C:/Users/Asus/.env")
load_dotenv("/root/sdr-vps/.env")

IMAP_HOST = os.getenv("LEADS_IMAP_HOST", "mail.manddigital.com.br")
IMAP_USER = os.getenv("LEADS_IMAP_USER") or os.getenv("OUTLOOK_USER")
IMAP_PASS = os.getenv("LEADS_IMAP_PASS") or os.getenv("OUTLOOK_PASS")
PD = os.getenv("PIPEDRIVE_API_TOKEN")
PIPELINE_ID = int(os.getenv("EMAIL_LEADS_PIPELINE_ID", "7"))
STAGE_ID = int(os.getenv("EMAIL_LEADS_STAGE_ID", "52"))
TAG_NAME = "LEAD-TRÁFEGO"
API = "https://api.pipedrive.com/v1"
PROCESSED = Path("/root/sdr-vps/data/email_inbox_processed.txt")
WA_STATE_FILE = Path("/root/sdr-vps/data/whatsapp_warm_cadence.json")

# Helpers for Pipedrive API
def pd_api(method, path, body=None, params=None):
    params = params or {}
    params["api_token"] = PD
    r = requests.request(method, f"{API}{path}", params=params, json=body, timeout=30)
    if r.status_code >= 400:
        print(f"[PD_API_ERROR] {method} {path} {r.status_code} {r.text[:200]}")
    r.raise_for_status()
    return r.json()

def normalize_phone(phone):
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if digits.startswith("55") and len(digits) >= 12:
        return digits
    if len(digits) in (10, 11):
        return "55" + digits
    return digits

def is_valid_org_name(name):
    if not name: return False
    # Reject if it contains CSS/HTML artifacts
    if "{" in name or "}" in name or "padding:" in name or "#outlook" in name or "<style" in name or "@media" in name:
        return False
    # Reject if it's too long and looks like code or junk
    if len(name) > 100 and (";" in name or "body" in name):
        return False
    return True

def clean_org_name(raw_name, full_text=""):
    name = str(raw_name or "").strip()
    if not is_valid_org_name(name):
        if is_valid_org_name(full_text): # Should not happen, but just in case
             name = full_text[:80]
        else:
             # Try to find a better one in the text if possible, but usually just reject
             return ""
    
    # Final cleanup of common junk
    name = re.sub(r"\s+", " ", name).strip(" -|")
    return name[:80]

def hdr(v):
    if not v: return ""
    out = ""
    for p, e in decode_header(v):
        if isinstance(p, bytes):
            out += p.decode(e or "utf-8", "ignore")
        else:
            out += p
    return out.strip()

def get_body(msg):
    parts = msg.walk() if msg.is_multipart() else [msg]
    for part in parts:
        if part.get_content_type() in ("text/plain", "text/html"):
            raw = part.get_payload(decode=True) or b""
            return raw.decode(part.get_content_charset() or "utf-8", "ignore")
    return ""

def clean_html_content(t):
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def extract_field(text, names):
    for n in names:
        # Match field name followed by : or - and capture until next field or end of line
        m = re.search(rf"{n}\s*[:\-]\s*(.+?)(?=\s+(?:Nome|Name|Email|E-mail|Telefone|Phone|WhatsApp|Empresa|Company|Mensagem|Message|Data|Horário|Horario|Demanda)\s*[:\-]|$)", text, re.I)
        if m: 
            val = clean_html_content(m.group(1))
            return val.strip()
    return ""

def extract_lead_info(text, sender_email):
    # Prepare text by converting <br> to \n for better field extraction
    clean_text = text.replace("<br>", "\n").replace("<br/>", "\n")
    clean_text = clean_html_content(clean_text)
    
    nome = extract_field(clean_text, ["nome", "name"])
    email_val = extract_field(clean_text, ["email", "e-mail"])
    phone = extract_field(clean_text, ["telefone", "phone", "whatsapp", "celular"])
    empresa = extract_field(clean_text, ["empresa", "company", "loja", "organização", "organizacao"])
    demanda = extract_field(clean_text, ["mensagem", "message", "demanda"])

    # Fallbacks
    if not email_val:
        found_emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', clean_text + " " + sender_email)
        email_val = next((x for x in found_emails if not any(b in x.lower() for b in ["leadster", "manddigital"])), "")
    
    if not phone:
        found_phones = re.findall(r'(?:\+?55)?\s?\(?\d{2}\)?\s?\d{4,5}[-\s]?\d{4}', clean_text)
        phone = found_phones[0] if found_phones else ""

    if not nome and email_val:
        nome = email_val.split("@")[0].replace(".", " ").replace("_", " ").title()

    return {
        "nome": (nome or "Lead sem nome").strip()[:100],
        "empresa": clean_org_name(empresa),
        "email": email_val.lower().strip(),
        "phone": normalize_phone(phone),
        "demanda": demanda.strip(),
        "texto_completo": clean_text[:3000]
    }

def get_or_create_org(name):
    if not name: return None
    if not is_valid_org_name(name):
        print("[ORG_INVALID_HTML]", name)
        return None
        
    try:
        data = pd_api("GET", "/organizations/search", params={"term": name, "exact_match": True}).get("data", {}).get("items", [])
        if data:
            org_id = data[0]["item"]["id"]
            print("[ORG_UPDATE_OK]", org_id, name)
            return org_id
        
        res = pd_api("POST", "/organizations", {"name": name})
        org_id = res["data"]["id"]
        print("[ORG_CREATE_OK]", org_id, name)
        return org_id
    except Exception as e:
        print("[ORG_ERROR]", name, str(e))
        return None

def trigger_warm_whatsapp(deal_id, phone, name, demand):
    if not phone or len(phone) < 10:
        print("[WA_WARM_TRIGGER_SKIP] Invalid phone:", phone)
        return False
    
    # Contextual message logic
    demand_lower = demand.lower()
    if "totem" in demand_lower:
        msg = f"Oi {name}, tudo bem? Aqui é a Carol da Mand Digital. 😊\n\nVi que você perguntou sobre a solução de totem digital para captação de leads. Posso te explicar como funciona e sobre a impressão do canhoto de sorteio?"
    elif len(demand) > 10:
        # Contextual but generic
        context = demand[:60] + "..." if len(demand) > 60 else demand
        msg = f"Oi {name}, tudo bem? Aqui é a Carol da Mand Digital. 😊\n\nRecebi seu contato sobre '{context}'. Como posso te ajudar com isso hoje?"
    else:
        # Generic Warm
        msg = f"Oi {name}, tudo bem? Aqui é a Carol da Mand Digital. 😊\n\nVi seu interesse em nossas soluções e gostaria de entender melhor seu projeto. Podemos falar?"

    try:
        # Use WhatsApp Gateway
        r = requests.post("http://127.0.0.1:3000/send", json={
            "number": phone,
            "text": msg
        }, timeout=30)
        
        if r.status_code == 200 and '"sent"' in r.text.lower():
            print("[WA_WARM_TRIGGER_OK]", deal_id, phone)
            # Update state for whatsapp_warm_cadence.py
            update_wa_state(deal_id, phone, name, msg)
            return True
        else:
            print("[WA_WARM_TRIGGER_FAIL]", deal_id, r.status_code, r.text[:100])
    except Exception as e:
        print("[WA_WARM_TRIGGER_ERR]", deal_id, str(e))
    
    return False

def update_wa_state(deal_id, phone, name, msg):
    try:
        state = {}
        if WA_STATE_FILE.exists():
            try:
                state = json.loads(WA_STATE_FILE.read_text())
            except:
                state = {}
        
        state[str(deal_id)] = {
            "step": 1,
            "last_sent_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "phone": phone,
            "name": name,
            "origin": "trafego",
            "last_message_preview": msg[:100],
            "wa1_sent": True,
            "last_sent_step_whatsapp": 1
        }
        WA_STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))
    except Exception as e:
        print("[WA_STATE_UPDATE_ERR]", str(e))

def find_open_deal(person_id):
    if not person_id: return None
    try:
        deals = pd_api("GET", f"/persons/{person_id}/deals", params={"status": "open"}).get("data") or []
        if deals:
            # Return the oldest open deal
            deals = sorted(deals, key=lambda d: int(d.get("id") or 0))
            return deals[0]
    except:
        pass
    return None

def process_lead(lead, subject, mid_s=None):
    print("[LEAD_PARSE_OK]", lead["email"] or lead["phone"], lead["nome"], lead["empresa"])

    if not lead["email"] and not lead["phone"]:
        print("[LEAD_SKIP_NO_CONTACT]")
        return False

    # Org Handling
    org_id = get_or_create_org(lead["empresa"])

    # Person Handling
    existing_person = None
    term = lead["email"] or lead["phone"]
    if term:
        search_res = pd_api("GET", "/persons/search", params={"term": term, "exact_match": True}).get("data", {}).get("items", [])
        if search_res:
            existing_person = search_res[0]["item"]

    if existing_person:
        person_id = existing_person["id"]
        print("[PERSON_UPDATE_OK]", person_id, term)
        # Ensure org is linked if missing
        if org_id and not existing_person.get("organization"):
                pd_api("PUT", f"/persons/{person_id}", {"org_id": org_id})
    else:
        person_id = pd_api("POST", "/persons", {
            "name": lead["nome"],
            "org_id": org_id,
            "email": [{"value": lead["email"], "primary": True}] if lead["email"] else [],
            "phone": [{"value": lead["phone"], "primary": True}] if lead["phone"] else []
        })["data"]["id"]
        print("[PERSON_CREATE_OK]", person_id, lead["nome"])

    # Deal Handling
    deal = find_open_deal(person_id)
    if deal:
        deal_id = deal["id"]
        print("[DEAL_ALREADY_EXISTS]", deal_id)
    else:
        title = f"Lead LP - {lead['empresa'] or lead['nome']}"
        res_deal = pd_api("POST", "/deals", {
            "title": title[:150],
            "person_id": person_id,
            "org_id": org_id,
            "pipeline_id": PIPELINE_ID,
            "stage_id": STAGE_ID,
            "label": 193 # LEAD-TRÁFEGO
        })
        deal_id = res_deal["data"]["id"]
        print("[DEAL_CREATE_OK]", deal_id, title)

    # Note Handling
    note_content = f"<b>Origem:</b> Leadster/Formulário LP<br><b>Assunto:</b> {subject}<br><b>Demanda:</b> {lead['demanda']}<br><br>---<br>{lead['texto_completo'].replace(chr(10),'<br>')}"
    pd_api("POST", "/notes", {
        "deal_id": deal_id,
        "person_id": person_id,
        "org_id": org_id,
        "content": note_content
    })
    print("[NOTE_CREATE_OK]", deal_id)

    # WhatsApp Trigger
    trigger_warm_whatsapp(deal_id, lead["phone"], lead["nome"], lead["demanda"])
    return True

def main():
    print("[EMAIL_IMPORT_START]")
    if not IMAP_USER or not IMAP_PASS or not PD:
        print("[IMAP_ERROR] Missing credentials in .env (USER/PASS/PD)")
        # If in test mode, continue anyway
        if "--test-gabriel" not in sys.argv:
            return
    
    if "--test-gabriel" in sys.argv:
        print("[TEST_MODE] Simulating Gabriel lead...")
        mock_text = """
        Nome: Gabriel
        Email: gabriel.test@ademicon.com.br
        Telefone: 11988887777
        Empresa: Ademicon
        Demanda: Precisava saber se vocês tem alguma solução de totem digital para captação de leads que após computado, imprima um canhoto de sorteio também impresso.
        """
        lead = extract_lead_info(mock_text, "test@ademicon.com.br")
        process_lead(lead, "Novo lead gerado [TEST]")
        print("[TEST_MODE] Gabriel lead processed.")
        if "--test-only" in sys.argv:
            return

    processed = set(PROCESSED.read_text().splitlines()) if PROCESSED.exists() else set()

    try:
        imap = imaplib.IMAP4_SSL(IMAP_HOST, 993)
        imap.login(IMAP_USER, IMAP_PASS)
        imap.select("INBOX")
    except Exception as e:
        print("[IMAP_ERROR]", str(e))
        return

    _, data = imap.search(None, "ALL")
    # Process only the last 50 emails to be efficient
    for mid in data[0].split()[-50:]:
        mid_s = mid.decode()
        if mid_s in processed: continue

        try:
            _, raw = imap.fetch(mid, "(RFC822)")
            msg = email.message_from_bytes(raw[0][1])
            sender = hdr(msg.get("From"))
            subject = hdr(msg.get("Subject"))
            text = get_body(msg)

            # Check if it's a lead email
            is_lead = ("leads@leadster.com.br" in sender.lower() and "novo lead gerado" in subject.lower()) \
                      or ("comercial@manddigital.com.br" in sender.lower() and "formulário lp" in subject.lower())

            if not is_lead:
                processed.add(mid_s); continue

            lead = extract_lead_info(text, sender)
            if process_lead(lead, subject, mid_s):
                processed.add(mid_s)
                time.sleep(.5)

        except Exception as e:
            print("[IMPORT_LOOP_ERROR]", mid_s, str(e))

    # Save state
    PROCESSED.parent.mkdir(exist_ok=True, parents=True)
    PROCESSED.write_text("\n".join(sorted(processed, key=lambda x: int(x) if x.isdigit() else 0)))
    imap.logout()
    print("[EMAIL_IMPORT_DONE]")

if __name__ == "__main__":
    main()
