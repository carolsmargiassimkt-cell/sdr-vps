import os, re, imaplib, email, requests, time, json, sys
import unicodedata
from pathlib import Path
from email.header import decode_header
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_args, **_kwargs):
        return False

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.locked_json_state import locked_load_json, locked_save_json
from core.pipedrive_safe import safe_pd_request

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
WA_WARM_DELIVERY_WATCH_FILE = Path("/root/sdr-vps/data/wa_warm_delivery_watch.json")

# Helpers for Pipedrive API
def pd_api(method, path, body=None, params=None):
    return safe_pd_request(method, path, body, params) or {}

def is_dry_run_mode():
    return "--dry-run" in sys.argv or "--mock-only" in sys.argv or "--test-only" in sys.argv or "--test-gabriel" in sys.argv

def is_mock_mode():
    return "--mock-only" in sys.argv or "--test-gabriel" in sys.argv

def is_blocked_test_email(value):
    email_value = str(value or "").strip().lower()
    return email_value.startswith("gabriel.test@") or ".test@" in email_value or email_value.endswith("@example.com")

def normalize_phone(phone):
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0800") or len(digits) > 13:
        return ""
    if digits.startswith("55") and len(digits) in (12, 13):
        national = digits[2:]
        if len(national) not in (10, 11):
            return ""
        digits = "55" + national
    elif len(digits) in (10, 11):
        digits = "55" + digits
    else:
        return ""
    national = digits[2:]
    ddd = national[:2]
    if ddd == "00" or len(set(national)) <= 2:
        return ""
    return digits

def normalize_text_token(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip().lower()

def is_allowed_lead_email(sender, subject):
    sender_l = str(sender or "").lower()
    subject_n = normalize_text_token(subject)
    if "leads@leadster.com.br" in sender_l and "novo lead gerado" in subject_n:
        return True
    if "comercial@manddigital.com.br" in sender_l and subject_n in {"formulario lp", "home"}:
        return True
    return False

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
    t = re.sub(r"<br\s*/?>", "\n", str(t or ""), flags=re.I)
    t = re.sub(r"</(?:p|div|tr|li|table|section|article|h\d)>", "\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def extract_field(text, names):
    clean_names = {normalize_text_token(n) for n in names}
    field_names = "Nome|Name|Email|E-mail|Telefone|Phone|WhatsApp|Whatsapp|Celular|Empresa|Company|Mensagem|Message|Data|Horário|Horario|Demanda|Organização|Organizacao"
    for line in re.split(r"[\r\n]+", str(text or "")):
        clean_line = clean_html_content(line)
        if not clean_line or len(clean_line) > 500:
            continue
        m_line = re.match(rf"^\s*({field_names})\s*[:\-]\s*(.+?)\s*$", clean_line, flags=re.I)
        if m_line and normalize_text_token(m_line.group(1)) in clean_names:
            return clean_html_content(m_line.group(2)).strip()
    for n in names:
        # Match field name followed by : or - and capture until next field or end of line
        m = re.search(rf"{n}\s*[:\-]\s*(.+?)(?=\s+(?:Nome|Name|Email|E-mail|Telefone|Phone|WhatsApp|Empresa|Company|Mensagem|Message|Data|Horário|Horario|Demanda)\s*[:\-]|$)", text, re.I)
        if m: 
            val = clean_html_content(m.group(1))
            return val.strip()
    return ""

def extract_explicit_phone(raw_text, clean_text):
    for source in (raw_text, clean_text):
        value = extract_field(source, ["telefone", "phone", "whatsapp", "celular"])
        if value:
            phone = normalize_phone(value)
            if phone:
                return phone
            for candidate in re.findall(r'(?:\+?55\s*)?\(?\d{2}\)?\s*\d{4,5}[-\s]?\d{4}', value):
                phone = normalize_phone(candidate)
                if phone:
                    return phone
            print("[LEAD_INVALID_PHONE_FIELD]", str(value)[:120])
    return ""

def extract_lead_info(text, sender_email):
    # Prepare text by converting <br> to \n for better field extraction
    raw_text = str(text or "").replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    clean_text = clean_html_content(raw_text)
    
    nome = extract_field(raw_text, ["nome", "name"])
    email_val = extract_field(raw_text, ["email", "e-mail"])
    phone = extract_explicit_phone(raw_text, clean_text)
    empresa = extract_field(clean_text, ["empresa", "company", "loja", "organização", "organizacao"])
    demanda = extract_field(clean_text, ["mensagem", "message", "demanda"])

    # Fallbacks
    if not email_val:
        found_emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', clean_text + " " + sender_email)
        email_val = next((x for x in found_emails if not any(b in x.lower() for b in ["leadster", "manddigital"])), "")
    
    if not nome and email_val:
        nome = email_val.split("@")[0].replace(".", " ").replace("_", " ").title()

    return {
        "nome": (nome or "Lead sem nome").strip()[:100],
        "empresa": clean_org_name(empresa),
        "email": email_val.lower().strip(),
        "phone": phone,
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

def add_wa_warm_delivery_watch(*, deal_id, person_id=0, org_id=0, phone="", message_id="", text="", source="leadster_import"):
    if not deal_id or not message_id:
        return False
    now = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    state = locked_load_json(WA_WARM_DELIVERY_WATCH_FILE, {})
    if not isinstance(state, dict):
        state = {}
    key = str(message_id)
    previous = state.get(key) if isinstance(state.get(key), dict) else {}
    state[key] = {
        **previous,
        "deal_id": int(deal_id or previous.get("deal_id") or 0),
        "person_id": int(person_id or previous.get("person_id") or 0),
        "org_id": int(org_id or previous.get("org_id") or 0),
        "phone": normalize_phone(phone or previous.get("phone")),
        "message_id": key,
        "sent_at": previous.get("sent_at") or now,
        "status": previous.get("status") or "PENDING",
        "source": source or previous.get("source") or "leadster_import",
        "fallback_done": bool(previous.get("fallback_done") or False),
        "text": text or previous.get("text") or "",
        "updated_at": now,
    }
    locked_save_json(WA_WARM_DELIVERY_WATCH_FILE, state)
    print("[WA_WARM_DELIVERY_WATCH_ADD]", deal_id, phone, message_id)
    return True


def trigger_warm_whatsapp(deal_id, phone, name, demand, person_id=0, org_id=0):
    if is_dry_run_mode():
        print("[DRY_RUN_SKIP_WRITE] action=trigger_warm_whatsapp deal_id=", deal_id, "phone=", phone)
        return True
    if not phone or len(phone) < 10:
        print("[WA_WARM_TRIGGER_SKIP] Invalid phone:", phone)
        return False
    update_wa_state(deal_id, phone, name, "")
    note_payload = {
        "deal_id": int(deal_id),
        "content": "[WA_WARM_QUEUED] Lead de LP/Leadster elegivel para abertura WhatsApp warm. Envio oficial: scripts/whatsapp_warm_cadence.py.",
    }
    if int(person_id or 0):
        note_payload["person_id"] = int(person_id or 0)
    if int(org_id or 0):
        note_payload["org_id"] = int(org_id or 0)
    pd_api("POST", "/notes", note_payload)
    print("[WA_WARM_QUEUED]", deal_id, phone)
    return True

def update_wa_state(deal_id, phone, name, msg):
    if is_dry_run_mode():
        print("[DRY_RUN_SKIP_WRITE] action=update_wa_state deal_id=", deal_id, "phone=", phone)
        return
    try:
        state = locked_load_json(WA_STATE_FILE, {})
        if not isinstance(state, dict):
            state = {}
        
        previous = state.get(str(deal_id)) if isinstance(state.get(str(deal_id)), dict) else {}
        if int(previous.get("step") or 0) > 0 or previous.get("wa1_sent"):
            print("[WA_STATE_KEEP_EXISTING]", deal_id, phone, "step", previous.get("step"))
            return
        state[str(deal_id)] = {
            "step": 0,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "phone": phone,
            "name": name,
            "origin": "trafego",
            "last_message_preview": msg[:100],
            "wa1_sent": False,
            "last_sent_step_whatsapp": 0,
            "lead_replied": False,
            "replied": False
        }
        locked_save_json(WA_STATE_FILE, state)
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

def search_person_by_term(term):
    clean = str(term or "").strip()
    if not clean:
        return None
    try:
        items = pd_api("GET", "/persons/search", params={"term": clean, "exact_match": True}).get("data", {}).get("items", [])
        if items:
            return items[0]["item"]
    except Exception as exc:
        print("[PERSON_SEARCH_ERROR]", clean, str(exc))
    return None

def find_existing_person(email_value, phone_value):
    return search_person_by_term(email_value) or search_person_by_term(phone_value)

def process_lead(lead, subject, mid_s=None):
    print("[LEAD_PARSE_OK]", lead["email"] or lead["phone"], lead["nome"], lead["empresa"])
    if is_dry_run_mode():
        print("[DRY_RUN_SKIP_WRITE] action=process_lead email=", lead.get("email"), "phone=", lead.get("phone"))
        return True
    if is_blocked_test_email(lead.get("email")):
        print("[DRY_RUN_SKIP_WRITE] action=blocked_test_email email=", lead.get("email"))
        return False

    if not lead["email"] and not lead["phone"]:
        print("[LEAD_SKIP_NO_CONTACT]")
        return False

    # Org Handling
    org_id = get_or_create_org(lead["empresa"])

    # Person Handling
    existing_person = find_existing_person(lead["email"], lead["phone"])
    term = lead["email"] or lead["phone"]

    if existing_person:
        person_id = existing_person["id"]
        print("[PERSON_UPDATE_OK]", person_id, term)
        # Ensure org is linked if missing
        if org_id and not existing_person.get("organization"):
                pd_api("PUT", f"/persons/{person_id}", {"org_id": org_id})
    else:
        person_payload = {
            "name": lead["nome"],
            "org_id": org_id,
            "email": [{"value": lead["email"], "primary": True}] if lead["email"] else [],
            "phone": [{"value": lead["phone"], "primary": True}] if lead["phone"] else []
        }
        person_id = pd_api("POST", "/persons", person_payload)["data"]["id"]
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
    trigger_warm_whatsapp(deal_id, lead["phone"], lead["nome"], lead["demanda"], person_id=person_id, org_id=org_id or 0)
    return True

def main():
    print("[EMAIL_IMPORT_START]")
    if not IMAP_USER or not IMAP_PASS or not PD:
        print("[IMAP_ERROR] Missing credentials in .env (USER/PASS/PD)")
        # If in test mode, continue anyway
        if "--test-gabriel" not in sys.argv:
            return
    
    if "--test-gabriel" in sys.argv:
        print("[MOCK_MODE_ACTIVE] test_gabriel")
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
        if is_mock_mode() or "--test-only" in sys.argv:
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

            if not is_allowed_lead_email(sender, subject):
                print("[EMAIL_RUIDO]", sender, subject)
                processed.add(mid_s); continue

            lead = extract_lead_info(text, sender)
            if process_lead(lead, subject, mid_s):
                processed.add(mid_s)
                time.sleep(.5)

        except Exception as e:
            print("[IMPORT_LOOP_ERROR]", mid_s, str(e))

    # Save state
    if is_dry_run_mode():
        print("[DRY_RUN_SKIP_WRITE] action=save_processed")
    else:
        PROCESSED.parent.mkdir(exist_ok=True, parents=True)
        PROCESSED.write_text("\n".join(sorted(processed, key=lambda x: int(x) if x.isdigit() else 0)))
    imap.logout()
    print("[EMAIL_IMPORT_DONE]")

if __name__ == "__main__":
    main()
