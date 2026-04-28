import csv, os, re, time, requests

API="https://api.pipedrive.com/v1"
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
CSV="/root/sdr-vps/runtime/manual_enrich_clean.csv"
PIPELINE_ID=2

def digits(v):
    return re.sub(r"\D+","",str(v or ""))

def norm(s):
    return re.sub(r"[^a-z0-9]+"," ",str(s or "").lower()).strip()

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
    for f in paged("organizationFields"):
        if "cnpj" in str(f.get("name","")).lower():
            return f.get("key")
    raise SystemExit("Campo CNPJ não encontrado")

def org_id_from_deal(d):
    org=d.get("org_id")
    if isinstance(org,dict): return int(org.get("value") or 0)
    return int(org or 0)

def person_id_from_deal(d):
    p=d.get("person_id")
    if isinstance(p,dict): return int(p.get("value") or 0)
    return int(p or 0)

def phone_values(person):
    vals=[]
    for item in person.get("phone") or []:
        if isinstance(item,dict):
            v=digits(item.get("value"))
            if v: vals.append(v)
    return vals

def create_person(org_id,name,phone):
    body=req("POST","persons",json={
        "name":f"Contato {name}",
        "org_id":org_id,
        "phone":[{"value":phone,"primary":True}]
    })
    return body.get("data") or {}

def update_person_phone(person,phone):
    current=person.get("phone") or []
    existing=phone_values(person)
    if phone in existing:
        return "skip_phone_exists"
    new=current + [{"value":phone,"primary":False}]
    req("PUT",f"persons/{person['id']}",json={"phone":new})
    return "phone_added"

def attach_person(deal_id,pid):
    req("PUT",f"deals/{deal_id}",json={"person_id":pid})

def update_org_cnpj_if_empty(org,cnpj,field):
    current=digits(org.get(field))
    if current == cnpj:
        return "cnpj_equal"
    if current:
        return "cnpj_conflict_keep_existing"
    try:
        req("PUT",f"organizations/{org['id']}",json={field:cnpj})
        return "cnpj_filled"
    except Exception as e:
        print("CNPJ_FILL_ERROR:", org.get("id"), cnpj, str(e))
        return "cnpj_fill_error_continue"

if not TOKEN:
    raise SystemExit("PIPEDRIVE_API_TOKEN vazio")

cnpj_field=discover_cnpj_field()
print("[CNPJ_FIELD]",cnpj_field)

print("[LOAD DEALS]")
deals=paged("deals",{"status":"open","pipeline_id":PIPELINE_ID})
deal_index={}
for d in deals:
    title=norm(d.get("title","").replace(" - Deal","").replace(" - Lead",""))
    if title:
        deal_index.setdefault(title,[]).append(d)

ok_created=0
ok_phone_added=0
skip_phone_exists=0
review_no_deal=0
review_multi_deal=0
review_missing_org=0
cnpj_conflict=0
cnpj_filled=0

with open(CSV,newline="",encoding="utf-8") as f:
    for row in csv.DictReader(f):
        name=row["empresa"].strip()
        cnpj=digits(row["cnpj"])
        phone=digits(row["telefone"])
        key=norm(name)

        matches=[]
        for k,ds in deal_index.items():
            if key == k or key in k or k in key:
                matches.extend(ds)

        # dedup deal id
        byid={int(d["id"]):d for d in matches}
        matches=list(byid.values())

        if not matches:
            print("NO_DEAL_BY_NAME:",name)
            review_no_deal+=1
            continue
        if len(matches)>1:
            print("MULTI_DEAL_BY_NAME:",name,[d.get("id") for d in matches[:10]])
            review_multi_deal+=1
            continue

        deal=matches[0]
        deal_id=int(deal["id"])
        org_id=org_id_from_deal(deal)
        person_id=person_id_from_deal(deal)

        if not org_id:
            print("MISSING_ORG:",name,"deal",deal_id)
            review_missing_org+=1
            continue

        org=(req("GET",f"organizations/{org_id}").get("data") or {})
        cnpj_status=update_org_cnpj_if_empty(org,cnpj,cnpj_field)
        if cnpj_status=="cnpj_conflict_keep_existing":
            cnpj_conflict+=1
        elif cnpj_status=="cnpj_filled":
            cnpj_filled+=1

        if person_id:
            person=(req("GET",f"persons/{person_id}").get("data") or {})
            result=update_person_phone(person,phone)
            if result=="phone_added":
                ok_phone_added+=1
                print("PHONE_ADDED:",name,"deal",deal_id,"person",person_id,"cnpj_status",cnpj_status)
            else:
                skip_phone_exists+=1
                print("SKIP_PHONE_EXISTS:",name,"deal",deal_id,"person",person_id,"cnpj_status",cnpj_status)
        else:
            person=create_person(org_id,name,phone)
            pid=int(person.get("id") or 0)
            if pid:
                attach_person(deal_id,pid)
                ok_created+=1
                print("PERSON_CREATED:",name,"deal",deal_id,"org",org_id,"person",pid,"cnpj_status",cnpj_status)

        time.sleep(0.3)

print("[FINAL_COUNTS]")
print("person_created=",ok_created)
print("phone_added=",ok_phone_added)
print("skip_phone_exists=",skip_phone_exists)
print("review_no_deal=",review_no_deal)
print("review_multi_deal=",review_multi_deal)
print("review_missing_org=",review_missing_org)
print("cnpj_filled=",cnpj_filled)
print("cnpj_conflict_keep_existing=",cnpj_conflict)
