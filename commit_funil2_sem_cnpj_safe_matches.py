import csv, os, requests, re, time

API="https://api.pipedrive.com/v1"
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
IN="/root/sdr-vps/runtime/funil2_sem_cnpj_match_manual.csv"
CNPJ_FIELD="aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"

def digits(v):
    return re.sub(r"\D+","",str(v or ""))

def req(m,p,**k):
    params=k.pop("params",{}) or {}
    params["api_token"]=TOKEN
    r=requests.request(m,f"{API}/{p}",params=params,timeout=60,**k)
    r.raise_for_status()
    return r.json()

ok=0
skip=0

with open(IN, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        if r["match_status"] != "safe_match":
            continue

        org_id=int(r["org_id"])
        cnpj=digits(r["cnpj"])
        if not cnpj:
            skip+=1
            continue

        org=(req("GET",f"organizations/{org_id}").get("data") or {})
        atual=digits(org.get(CNPJ_FIELD))

        if atual:
            print("SKIP_HAS_CNPJ:",org_id,org.get("name"),atual)
            skip+=1
            continue

        try:
            req("PUT",f"organizations/{org_id}",json={CNPJ_FIELD:cnpj})
            print("CNPJ_FILLED:",org_id,org.get("name"),cnpj)
            ok+=1
        except Exception as e:
            print("CNPJ_FILL_ERROR:",org_id,org.get("name"),cnpj,str(e))
            skip+=1
        time.sleep(0.3)

print("[FINAL]")
print("cnpj_filled=",ok)
print("skip=",skip)
