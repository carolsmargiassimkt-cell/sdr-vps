import os, json, time, re, argparse, requests
from pathlib import Path

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"
PLAN=Path("waalaxy_writeback_plan.json")
STATE=Path("waalaxy_writeback_state.json")

BATCH_SIZE=25
SLEEP_BETWEEN_CALLS=1.5
SLEEP_BETWEEN_ITEMS=3

BLACKLIST_COMPANY=[
    "waalaxy international","kawaak"
]

def norm(s):
    return re.sub(r"\s+"," ",str(s or "").strip())

def key(s):
    return re.sub(r"[^a-z0-9]","",norm(s).lower())

def api(method,path,params=None,json_body=None,retries=0):
    if not TOKEN:
        raise SystemExit("SEM PIPEDRIVE_API_TOKEN")
    params=params or {}
    params["api_token"]=TOKEN
    try:
        r=requests.request(method,BASE+path,params=params,json=json_body,timeout=40)
    except Exception as e:
        if retries<3:
            wait=5*(retries+1)
            print("[RETRY_CONN]",path,wait,e)
            time.sleep(wait)
            return api(method,path,params,json_body,retries+1)
        raise

    if r.status_code==429:
        wait=min(int(r.headers.get("Retry-After","10") or 10),60)
        print("[RATE_LIMIT]",wait)
        time.sleep(wait)
        return api(method,path,params,json_body,retries+1)

    time.sleep(SLEEP_BETWEEN_CALLS)

    try:
        data=r.json()
    except Exception:
        data={"raw":r.text}

    if r.status_code>=400:
        print("[API_ERR]",method,path,r.status_code,data)
    return data

def search_org(company):
    data=api("GET","/organizations/search",params={"term":company,"exact_match":0,"limit":10})
    items=((data.get("data") or {}).get("items") or [])
    for it in items:
        o=it.get("item") or {}
        if key(o.get("name"))==key(company):
            return o
    return None

def create_org(company):
    data=api("POST","/organizations",json_body={"name":company})
    return data.get("data") or {}

def search_person(name, linkedin="", email=""):
    term=email or name
    data=api("GET","/persons/search",params={"term":term,"exact_match":0,"limit":10})
    items=((data.get("data") or {}).get("items") or [])
    for it in items:
        p=it.get("item") or {}
        if key(p.get("name"))==key(name):
            return p
    return None

def create_person(x, org_id):
    payload={"name":x["name"],"org_id":org_id}
    if x.get("email"):
        payload["email"]=[{"value":x["email"],"primary":True,"label":"work"}]
    if x.get("phone"):
        payload["phone"]=[{"value":x["phone"],"primary":True,"label":"work"}]
    data=api("POST","/persons",json_body=payload)
    return data.get("data") or {}

def update_person_org(person_id, org_id):
    api("PUT",f"/persons/{person_id}",json_body={"org_id":org_id})

def search_deal(company, person_name, org_id=None, person_id=None):
    terms=[company, person_name]
    seen=set()
    for term in terms:
        if not term: continue
        data=api("GET","/deals/search",params={"term":term,"status":"open","limit":10})
        items=((data.get("data") or {}).get("items") or [])
        for it in items:
            d=it.get("item") or {}
            did=d.get("id")
            if not did or did in seen: continue
            seen.add(did)

            detail=api("GET",f"/deals/{did}").get("data") or {}
            d_org=detail.get("org_id")
            d_person=detail.get("person_id")
            d_org_id=d_org.get("value") if isinstance(d_org,dict) else d_org
            d_person_id=d_person.get("value") if isinstance(d_person,dict) else d_person

            if org_id and str(d_org_id)==str(org_id):
                return detail
            if person_id and str(d_person_id)==str(person_id):
                return detail
    return None

def create_deal(x, org_id, person_id):
    title=f"{x['company']} - {x['name']}"
    payload={
        "title":title,
        "org_id":org_id,
        "person_id":person_id,
        "status":"open"
    }
    data=api("POST","/deals",json_body=payload)
    return data.get("data") or {}

def update_deal(deal_id, x, org_id, person_id):
    title=f"{x['company']} - {x['name']}"
    payload={"title":title,"org_id":org_id,"person_id":person_id}
    api("PUT",f"/deals/{deal_id}",json_body=payload)

def add_note(deal_id,x):
    content=(
        "<b>Importação Waalaxy/SalesNav</b><br>"
        f"Tipo: {x.get('tipo')}<br>"
        f"Waalaxy score: {x.get('waalaxy_score')}<br>"
        f"Pessoa: {x.get('name')}<br>"
        f"Empresa: {x.get('company')}<br>"
        f"Cargo: {x.get('title')}<br>"
        f"LinkedIn: {x.get('linkedin')}<br>"
        f"Arquivo: {x.get('source_file')}<br>"
        "Critério: score >= 50, sem REVIEW, import controlado sem duplicidade."
    )
    api("POST","/notes",json_body={"deal_id":deal_id,"content":content})

def load_state():
    if STATE.exists():
        return json.loads(STATE.read_text(encoding="utf-8"))
    return {"done":[]}

def save_state(st):
    STATE.write_text(json.dumps(st,ensure_ascii=False,indent=2),encoding="utf-8")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--apply",action="store_true")
    ap.add_argument("--limit",type=int,default=BATCH_SIZE)
    args=ap.parse_args()

    plan=json.loads(PLAN.read_text(encoding="utf-8"))
    rows=[
        x for x in plan
        if x.get("waalaxy_score",0)>=50
        and "REVIEW" not in x.get("action","")
        and x.get("action") == "CREATE_NEW"
        and key(x.get("company")) not in [key(b) for b in BLACKLIST_COMPANY]
    ]

    st=load_state()
    done=set(st.get("done",[]))

    pending=[]
    for x in rows:
        uid=key(x.get("linkedin") or (x.get("name","")+"|"+x.get("company","")))
        if uid not in done:
            x["_uid"]=uid
            pending.append(x)

    batch=pending[:args.limit]

    print("MODO:", "APPLY" if args.apply else "DRY_RUN")
    print("TOTAL_ELEGIVEL:",len(rows))
    print("JA_FEITO:",len(done))
    print("PENDENTE:",len(pending))
    print("LOTE_AGORA:",len(batch))

    for x in batch:
        print("\n---")
        print("PESSOA:",x["name"])
        print("EMPRESA:",x["company"])
        print("ACTION_PLAN:",x["action"])
        print("SCORE:",x["waalaxy_score"])

        if not args.apply:
            continue

        org_match = (x.get("crm_org_matches") or [{}])[0]
        if org_match.get("org_id"):
            org_id = org_match.get("org_id")
            print("[ORG_OK_EXPORT]", org_id, org_match.get("org"))
        else:
            org=search_org(x["company"])
            if org:
                org_id=org["id"]
                print("[ORG_OK_API]",org_id,org.get("name"))
            else:
                org=create_org(x["company"])
                org_id=org.get("id")
                print("[ORG_CREATE]",org_id,x["company"])

        person=search_person(x["name"],x.get("linkedin"),x.get("email"))
        if person:
            person_id=person["id"]
            print("[PERSON_OK]",person_id,person.get("name"))
            update_person_org(person_id,org_id)
            print("[PERSON_ORG_SET]",person_id,"->",org_id)
        else:
            person=create_person(x,org_id)
            person_id=person.get("id")
            print("[PERSON_CREATE]",person_id,x["name"])

        deal=search_deal(x["company"],x["name"],org_id=org_id,person_id=person_id)
        if deal:
            deal_id=deal["id"]
            update_deal(deal_id,x,org_id,person_id)
            print("[DEAL_UPDATE]",deal_id)
        else:
            deal=create_deal(x,org_id,person_id)
            deal_id=deal.get("id")
            print("[DEAL_CREATE]",deal_id)

        add_note(deal_id,x)
        print("[NOTE_OK]",deal_id)

        st["done"].append(x["_uid"])
        save_state(st)

        time.sleep(SLEEP_BETWEEN_ITEMS)

    print("\nFIM_LOTE")

if __name__=="__main__":
    main()
