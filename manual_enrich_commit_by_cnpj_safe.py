import csv, os, re, time, requests

API="https://api.pipedrive.com/v1"
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
CSV="/root/sdr-vps/runtime/manual_enrich_clean.csv"

def digits(v):
    return re.sub(r"\D+","",str(v or ""))

def req(m,p,**k):
    params=k.pop("params",{}) or {}
    params["api_token"]=TOKEN
    r=requests.request(m,f"{API}/{p}",params=params,timeout=60,**k)
    r.raise_for_status()
    return r.json()

def paged(path, params=None):
    out=[]; start=0
    while True:
        q=dict(params or {})
        q.update({"start":start,"limit":500})
        body=req("GET",path,params=q)
        data=body.get("data") or []
        out.extend(data)
        pg=(body.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"):
            break
        start=pg.get("next_start") or 0
        time.sleep(0.2)
    return out

def discover_cnpj_field():
    fields=paged("organizationFields")
    for f in fields:
        if "cnpj" in str(f.get("name","")).lower():
            return f.get("key")
    raise SystemExit("Campo CNPJ não encontrado em organizationFields")

def org_id_from_deal(d):
    org=d.get("org_id")
    if isinstance(org,dict):
        return int(org.get("value") or 0)
    return int(org or 0)

def person_id_from_deal(d):
    person=d.get("person_id")
    if isinstance(person,dict):
        return int(person.get("value") or 0)
    return int(person or 0)

def create_person(org_id,name,phone):
    body=req("POST","persons",json={
        "name":f"Contato {name}",
        "org_id":org_id,
        "phone":[{"value":phone,"primary":True}]
    })
    return body.get("data") or {}

def attach_person(deal_id,person_id):
    req("PUT",f"deals/{deal_id}",json={"person_id":person_id})

cnpj_field=discover_cnpj_field()
print("[CNPJ_FIELD]",cnpj_field)

print("[LOAD_ORGS]")
orgs=paged("organizations")
org_by_cnpj={}
for o in orgs:
    c=digits(o.get(cnpj_field))
    if c and c not in org_by_cnpj:
        org_by_cnpj[c]=o

print("[LOAD_DEALS_PIPELINE2]")
deals=paged("deals",{"status":"open","pipeline_id":2})
deals_by_org={}
for d in deals:
    oid=org_id_from_deal(d)
    if oid:
        deals_by_org.setdefault(oid,[]).append(d)

ok=0
skip_no_org=0
skip_no_deal=0
skip_has_person=0
review_multi_deal=0

with open(CSV, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        name=row["empresa"].strip()
        cnpj=digits(row["cnpj"])
        phone=digits(row["telefone"])

        org=org_by_cnpj.get(cnpj)
        if not org:
            print("NO_ORG_BY_CNPJ:",name,cnpj)
            skip_no_org+=1
            continue

        org_id=int(org.get("id") or 0)
        org_deals=deals_by_org.get(org_id,[])

        if not org_deals:
            print("NO_OPEN_DEAL_PIPELINE2:",name,cnpj,"org",org_id)
            skip_no_deal+=1
            continue

        if len(org_deals)>1:
            print("MULTI_OPEN_DEAL_PIPELINE2:",name,cnpj,"org",org_id,[d.get("id") for d in org_deals])
            review_multi_deal+=1
            continue

        deal=org_deals[0]
        deal_id=int(deal.get("id") or 0)

        if person_id_from_deal(deal):
            print("SKIP_HAS_PERSON:",name,cnpj,"deal",deal_id)
            skip_has_person+=1
            continue

        person=create_person(org_id,name,phone)
        pid=int(person.get("id") or 0)
        if pid:
            attach_person(deal_id,pid)
            print("OK_BY_CNPJ:",name,cnpj,"org",org_id,"deal",deal_id,"person",pid)
            ok+=1
        time.sleep(0.3)

print("[FINAL_COUNTS]")
print("ok_by_cnpj=",ok)
print("skip_no_org=",skip_no_org)
print("skip_no_deal=",skip_no_deal)
print("skip_has_person=",skip_has_person)
print("review_multi_deal=",review_multi_deal)
