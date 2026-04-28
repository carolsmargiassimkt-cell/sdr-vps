import csv, os, re, time, requests, difflib

API="https://api.pipedrive.com/v1"
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
CSV="/root/sdr-vps/runtime/manual_enrich_clean.csv"
PIPELINE_ID=2
DRY_RUN=os.getenv("DRY_RUN","1")!="0"

STOPWORDS=set("""
brasil brasileira brasileiro do da de das dos e em para com sem s a o os as
ltda sa s/a me eireli grupo comercio comércio industria indústria servicos serviços
participacoes participações holding empresa cia companhia center shopping supermercado supermercados
""".split())

def digits(v):
    return re.sub(r"\D+","",str(v or ""))

def norm(s):
    s=str(s or "").lower()
    s=s.replace("ç","c")
    s=re.sub(r"[áàãâä]","a",s)
    s=re.sub(r"[éèêë]","e",s)
    s=re.sub(r"[íìîï]","i",s)
    s=re.sub(r"[óòõôö]","o",s)
    s=re.sub(r"[úùûü]","u",s)
    s=re.sub(r"[^a-z0-9]+"," ",s)
    words=[w for w in s.split() if w not in STOPWORDS and len(w)>1]
    return " ".join(words)

def score(a,b):
    a=norm(a); b=norm(b)
    if not a or not b: return 0
    if a==b: return 100
    if a in b or b in a: return 92
    return int(difflib.SequenceMatcher(None,a,b).ratio()*100)

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
        time.sleep(0.15)
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
    payload={
        "name":f"Contato {name}",
        "org_id":org_id,
        "phone":[{"value":phone,"primary":True}]
    }
    if DRY_RUN:
        return {"id":-1}
    return req("POST","persons",json=payload).get("data") or {}

def update_person_phone(person,phone):
    current=person.get("phone") or []
    existing=phone_values(person)
    if phone in existing:
        return "skip_phone_exists"
    if DRY_RUN:
        return "would_add_phone"
    new=current + [{"value":phone,"primary":False}]
    req("PUT",f"persons/{person['id']}",json={"phone":new})
    return "phone_added"

def attach_person(deal_id,pid):
    if DRY_RUN:
        return
    req("PUT",f"deals/{deal_id}",json={"person_id":pid})

def update_org_cnpj_if_empty(org,cnpj,field):
    current=digits(org.get(field))
    if current == cnpj:
        return "cnpj_equal"
    if current:
        return "cnpj_conflict_keep_existing"
    if DRY_RUN:
        return "would_fill_cnpj"
    try:
        req("PUT",f"organizations/{org['id']}",json={field:cnpj})
        return "cnpj_filled"
    except Exception as e:
        print("CNPJ_FILL_ERROR:",org.get("id"),cnpj,str(e))
        return "cnpj_fill_error_continue"

def choose_deal(candidates):
    if not candidates:
        return None, "no_deal"
    # prioriza sem pessoa
    no_person=[d for d in candidates if not person_id_from_deal(d)]
    pool=no_person or candidates
    # se ainda muitos, pega maior id como mais recente
    pool=sorted(pool,key=lambda d:int(d.get("id") or 0),reverse=True)
    return pool[0], "chosen_no_person_or_latest"

if not TOKEN:
    raise SystemExit("PIPEDRIVE_API_TOKEN vazio")

cnpj_field=discover_cnpj_field()
print("[CNPJ_FIELD]",cnpj_field)
print("[DRY_RUN]",DRY_RUN)

print("[LOAD DEALS]")
deals=paged("deals",{"status":"open","pipeline_id":PIPELINE_ID})
for d in deals:
    d["_norm_title"]=norm(str(d.get("title","")).replace(" - Deal","").replace(" - Lead",""))

ok_created=0
ok_phone_added=0
would_created=0
would_phone_added=0
skip_phone_exists=0
review_no_match=0
review_low_score=0
review_missing_org=0
cnpj_conflict=0
cnpj_fill=0

with open(CSV,newline="",encoding="utf-8") as f:
    for row in csv.DictReader(f):
        name=row["empresa"].strip()
        cnpj=digits(row["cnpj"])
        phone=digits(row["telefone"])

        scored=[]
        for d in deals:
            s=score(name,d.get("title",""))
            if s>=82:
                scored.append((s,d))
        scored=sorted(scored,key=lambda x:(x[0],int(x[1].get("id") or 0)),reverse=True)

        if not scored:
            print("NO_FUZZY_MATCH:",name)
            review_no_match+=1
            continue

        top_score=scored[0][0]
        candidates=[d for s,d in scored if s==top_score]

        if top_score < 86:
            print("LOW_SCORE_REVIEW:",name,"score",top_score,"best",candidates[0].get("title"))
            review_low_score+=1
            continue

        deal,why=choose_deal(candidates)
        deal_id=int(deal.get("id") or 0)
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
        elif cnpj_status in {"cnpj_filled","would_fill_cnpj"}:
            cnpj_fill+=1

        if person_id:
            person=(req("GET",f"persons/{person_id}").get("data") or {})
            result=update_person_phone(person,phone)
            if result=="phone_added":
                ok_phone_added+=1
                print("PHONE_ADDED:",name,"=>",deal.get("title"),"score",top_score,"deal",deal_id,"person",person_id,"cnpj",cnpj_status)
            elif result=="would_add_phone":
                would_phone_added+=1
                print("WOULD_ADD_PHONE:",name,"=>",deal.get("title"),"score",top_score,"deal",deal_id,"person",person_id,"cnpj",cnpj_status)
            else:
                skip_phone_exists+=1
                print("SKIP_PHONE_EXISTS:",name,"=>",deal.get("title"),"score",top_score,"deal",deal_id,"person",person_id,"cnpj",cnpj_status)
        else:
            person=create_person(org_id,name,phone)
            pid=int(person.get("id") or 0)
            if DRY_RUN:
                would_created+=1
                print("WOULD_CREATE_PERSON:",name,"=>",deal.get("title"),"score",top_score,"deal",deal_id,"org",org_id,"cnpj",cnpj_status)
            elif pid:
                attach_person(deal_id,pid)
                ok_created+=1
                print("PERSON_CREATED:",name,"=>",deal.get("title"),"score",top_score,"deal",deal_id,"org",org_id,"person",pid,"cnpj",cnpj_status)

        time.sleep(0.2)

print("[FINAL_COUNTS]")
print("person_created=",ok_created)
print("phone_added=",ok_phone_added)
print("would_create_person=",would_created)
print("would_add_phone=",would_phone_added)
print("skip_phone_exists=",skip_phone_exists)
print("review_no_match=",review_no_match)
print("review_low_score=",review_low_score)
print("review_missing_org=",review_missing_org)
print("cnpj_fill_or_would_fill=",cnpj_fill)
print("cnpj_conflict_keep_existing=",cnpj_conflict)
