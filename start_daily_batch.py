import os,requests,random,time

PD=os.getenv("PIPEDRIVE_API_TOKEN")
PIPE=7

BAD_EMAIL_PREFIXES = (
    "fiscal@", "contabilidade@", "tributario@", "paralegal@", "legalizacao@",
    "nfe@", "financeiro@", "adm@", "administrativo@", "atendimento@",
    "sac@", "rh@", "dp@", "depto", "deptocontabil", "postmaster@",
    "no-reply@", "noreply@", "naoresponda@"
)

BAD_EMAIL_PARTS = (
    "contabil", "tributario", "fiscal", "legalizacao", "paralegal",
    "financeiro", "nfe", "cobranca", "compras"
)

def email_is_bad(email):
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return True
    if e.startswith(BAD_EMAIL_PREFIXES):
        return True
    if any(x in e for x in BAD_EMAIL_PARTS):
        return True
    return False

LIMIT=50

def get_deals():
    r=requests.get("https://api.pipedrive.com/v1/deals",
        params={"api_token":PD,"status":"open","limit":200}).json()
    return r.get("data") or []

def has_started(d):
    return "email_cad_1" in (d.get("label") or "")

def start_cadence(deal_id):
    requests.post("http://127.0.0.1:8001/webhooks/email-cadence",
        json={"deal_id":deal_id},timeout=10)

deals=get_deals()
random.shuffle(deals)

count=0
for d in deals:
    if int(d.get("pipeline_id") or 0)!=PIPE: continue
    if has_started(d): continue

    email = (
        d.get("person_id", {}).get("email", [{}])[0].get("value")
        if isinstance(d.get("person_id"), dict) else ""
    ) or d.get("email") or ""

    if email_is_bad(email):
        print("SKIP_BAD_EMAIL:", d.get("id"), email)
        continue

    start_cadence(d["id"])
    print("STARTED:",d["id"])
    count+=1
    time.sleep(1)

    if count>=LIMIT: break

print("TOTAL:",count)
