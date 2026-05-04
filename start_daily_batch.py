import os,requests,random,time

PD=os.getenv("PIPEDRIVE_API_TOKEN")
PIPE=7
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

    start_cadence(d["id"])
    print("STARTED:",d["id"])
    count+=1
    time.sleep(1)

    if count>=LIMIT: break

print("TOTAL:",count)
