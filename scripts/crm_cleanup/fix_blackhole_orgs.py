import os, csv, requests, re, sys
from pathlib import Path

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"
APPLY="--apply" in sys.argv

BLACKHOLE_ORGS={1991,2148,2149}
SRC=Path("reports/crm_audit/deals_person_org_inconsistente.csv")
OUT=Path("reports/crm_cleanup/fix_blackhole_orgs_plan.csv")

def clean_title(t):
    t=re.sub(r"\s*-\s*Deal\s*$","",t or "",flags=re.I).strip()
    return t[:255]

def api(method,path,**kw):
    params=kw.pop("params",{}) or {}
    params["api_token"]=TOKEN
    r=requests.request(method,BASE+"/"+path,params=params,timeout=30,**kw)
    r.raise_for_status()
    return r.json().get("data")

def find_org(name):
    data=api("GET","organizations/search",params={"term":name,"exact_match":1})
    items=(data or {}).get("items") or []
    for it in items:
        org=it.get("item") or {}
        if (org.get("name") or "").strip().lower()==name.strip().lower():
            return org.get("id")
    return None

def create_org(name):
    data=api("POST","organizations",json={"name":name})
    return data.get("id")

rows=list(csv.DictReader(open(SRC,encoding="utf-8")))
plan=[]

for r in rows:
    if r.get("status")!="open": 
        continue
    if str(r.get("pipeline_id"))!="7":
        continue
    if int(r.get("org_id") or 0) not in BLACKHOLE_ORGS:
        continue
    deal_id=int(r["deal_id"])
    old_org=int(r["org_id"])
    new_name=clean_title(r["deal_title"])
    if not new_name:
        continue
    new_org=find_org(new_name)
    action="reuse_org" if new_org else "create_org"
    if APPLY and not new_org:
        new_org=create_org(new_name)
    if APPLY and new_org:
        api("PUT",f"deals/{deal_id}",json={"org_id":new_org})
    plan.append({
        "deal_id":deal_id,
        "deal_title":r["deal_title"],
        "old_org_id":old_org,
        "old_org_name":r["org_name"],
        "new_org_name":new_name,
        "new_org_id":new_org or "",
        "action":action,
        "applied":APPLY,
    })

OUT.parent.mkdir(parents=True,exist_ok=True)
with open(OUT,"w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=plan[0].keys() if plan else ["empty"])
    w.writeheader()
    w.writerows(plan)

print(f"[PLAN_WRITTEN] {OUT} rows={len(plan)} apply={APPLY}")
