import csv, requests, os, time

API="https://api.pipedrive.com/v1"
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")

def req(m,p,**k):
    params=k.pop("params",{})
    params["api_token"]=TOKEN
    r=requests.request(m,f"{API}/{p}",params=params,timeout=60,**k)
    r.raise_for_status()
    return r.json()

def find_org(name):
    r=req("GET","organizations/search",params={"term":name,"limit":1})
    d=r.get("data",{}).get("items",[])
    return d[0]["item"]["id"] if d else None

def create_person(org_id,name,phone):
    return req("POST","persons",json={
        "name":f"Contato {name}",
        "org_id":org_id,
        "phone":[{"value":phone,"primary":True}]
    }).get("data",{})

def attach(deal_id,person_id):
    req("PUT",f"deals/{deal_id}",json={"person_id":person_id})

def find_deal(org_id):
    r=req("GET","deals",params={"org_id":org_id,"status":"open"})
    d=r.get("data",[])
    return d[0]["id"] if d else None

with open("/root/sdr-vps/runtime/manual_enrich_clean.csv") as f:
    reader=csv.DictReader(f)
    for row in reader:
        name=row["empresa"]
        phone=row["telefone"]

        org_id=find_org(name)
        if not org_id:
            print("ORG NAO ENCONTRADA:",name)
            continue

        person=create_person(org_id,name,phone)
        pid=person.get("id")

        deal_id=find_deal(org_id)
        if deal_id and pid:
            attach(deal_id,pid)

        print("OK:",name,org_id,pid)
        time.sleep(0.3)
