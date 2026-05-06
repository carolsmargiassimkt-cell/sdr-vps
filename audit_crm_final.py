import os, requests, collections

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"

if not TOKEN:
    raise SystemExit("ERRO: PIPEDRIVE_API_TOKEN vazio")

def get(path, params=None):
    params=params or {}
    params["api_token"]=TOKEN
    r=requests.get(BASE+path, params=params, timeout=30)
    print("GET", path, r.status_code)
    r.raise_for_status()
    return r.json().get("data") or []

def page_deals():
    out=[]; start=0
    while True:
        data=get("/deals", {"status":"open","pipeline_id":7,"start":start,"limit":500})
        if not data: break
        out += data
        if len(data)<500: break
        start += 500
    return out

deals=page_deals()
orgs=collections.Counter()
persons=collections.Counter()
sem_email=[]

for d in deals:
    did=d.get("id")
    title=d.get("title")
    org=d.get("org_id")
    person=d.get("person_id")

    oid=org.get("value") if isinstance(org,dict) else org
    pid=person.get("value") if isinstance(person,dict) else person

    if oid: orgs[oid]+=1
    if pid: persons[pid]+=1

    email_ok=False
    if pid:
        try:
            p=get(f"/persons/{pid}")
            emails=p.get("email") or []
            email_ok=any(e.get("value") for e in emails if isinstance(e,dict))
        except Exception as e:
            print("ERRO_PERSON", did, pid, e)

    if not email_ok:
        sem_email.append((did,title,pid,oid))

suspeitos=[]
for oid,c in orgs.items():
    if c>=20: suspeitos.append(("ORG_MUITOS_DEALS",oid,c))
for pid,c in persons.items():
    if c>=10: suspeitos.append(("PERSON_MUITOS_DEALS",pid,c))

print("\n=== RESULTADO ===")
print("DEALS_OPEN_PIPELINE_7:",len(deals))
print("ORGS_UNICAS:",len(orgs))
print("PERSONS_UNICAS:",len(persons))
print("SEM_EMAIL:",len(sem_email))
print("SUSPEITOS:",len(suspeitos))

print("\nTOP_ORGS:")
for oid,c in orgs.most_common(20): print(oid,c)

print("\nTOP_PERSONS:")
for pid,c in persons.most_common(20): print(pid,c)

print("\nSEM_EMAIL_AMOSTRA:")
for x in sem_email[:50]: print(x)

print("\nSUSPEITOS:")
for x in suspeitos: print(x)
