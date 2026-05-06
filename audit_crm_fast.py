import os, requests, collections
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"

def get(path, params=None):
    params=params or {}
    params["api_token"]=TOKEN
    r=requests.get(BASE+path, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("data") or []

out=[]; start=0
while True:
    data=get("/deals", {"status":"open","pipeline_id":7,"start":start,"limit":500})
    if not data: break
    out += data
    if len(data)<500: break
    start += 500

orgs=collections.Counter()
persons=collections.Counter()

for d in out:
    org=d.get("org_id")
    person=d.get("person_id")
    oid=org.get("value") if isinstance(org,dict) else org
    pid=person.get("value") if isinstance(person,dict) else person
    if oid: orgs[oid]+=1
    if pid: persons[pid]+=1

sus=[]
sus += [("ORG",k,v) for k,v in orgs.items() if v>=20]
sus += [("PERSON",k,v) for k,v in persons.items() if v>=10]

print("DEALS_OPEN_PIPELINE_7:", len(out))
print("ORGS_UNICAS:", len(orgs))
print("PERSONS_UNICAS:", len(persons))
print("SUSPEITOS:", len(sus))

print("\nTOP_ORGS")
for x in orgs.most_common(20): print(x)

print("\nTOP_PERSONS")
for x in persons.most_common(20): print(x)

print("\nSUSPEITOS")
for x in sus: print(x)
