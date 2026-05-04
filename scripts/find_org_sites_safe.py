import os,re,csv,time,requests
from urllib.parse import urlparse

TOKEN=os.getenv("PD_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"

BAD = ["duckduckgo","facebook","instagram","linkedin","reclameaqui","econodata","cnpj","jusbrasil","escavador"]

def pd(path,params=None):
    params=params or {}
    params["api_token"]=TOKEN
    return requests.get(BASE+path,params=params,timeout=30).json()

def get_orgs():
    out=[]; start=0
    while True:
        r=pd("/organizations",{"start":start,"limit":500})
        out+=r.get("data") or []
        pg=(r.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"): break
        start=pg.get("next_start",0)
    return out

def search(name):
    q = f"https://duckduckgo.com/?q={name}+site+oficial"
    html = requests.get(q,headers={"User-Agent":"Mozilla/5.0"},timeout=20).text

    links = re.findall(r'https://[a-zA-Z0-9\.\-]+\.[a-zA-Z]{2,}', html)

    for link in links:
        host = urlparse(link).netloc.replace("www.","").lower()

        if any(b in host for b in BAD):
            continue

        if len(host.split(".")) < 2:
            continue

        return host

    return ""

orgs = get_orgs()
rows = []

for o in orgs:
    name = o.get("name","")
    if not name:
        continue

    domain = search(name)

    print(name,"=>",domain)

    rows.append([o.get("id"), name, domain])
    time.sleep(1.2)

with open("runtime/org_sites_safe.csv","w",newline="",encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["org_id","empresa","dominio"])
    w.writerows(rows)

print("OK CSV GERADO")
