import os,csv,requests,time

TOKEN=os.getenv("PD_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"

def get_orgs():
    out=[]; start=0
    while True:
        r=requests.get(BASE+"/organizations",params={"api_token":TOKEN,"start":start,"limit":500}).json()
        out+=r.get("data") or []
        pg=(r.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"): break
        start=pg.get("next_start",0)
    return out

def find_domain(name):
    try:
        url=f"https://autocomplete.clearbit.com/v1/companies/suggest?query={name}"
        r=requests.get(url,timeout=10).json()
        if r:
            return r[0].get("domain",""), r[0].get("name","")
    except:
        pass
    return "",""

orgs=get_orgs()
rows=[]

for o in orgs:
    name=o.get("name","")
    if not name:
        continue

    domain,found_name = find_domain(name)

    print(name,"=>",domain)

    rows.append([o.get("id"), name, domain, found_name])
    time.sleep(0.3)

with open("runtime/org_sites_clearbit.csv","w",newline="",encoding="utf-8") as f:
    w=csv.writer(f)
    w.writerow(["org_id","empresa","dominio","nome_encontrado"])
    w.writerows(rows)

print("CSV GERADO")
