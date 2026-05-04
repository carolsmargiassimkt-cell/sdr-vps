import os,re,csv,time,requests
from urllib.parse import quote_plus,urlparse

TOKEN=os.getenv("PD_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
assert TOKEN, "export PD_TOKEN=SEU_TOKEN"

BASE="https://api.pipedrive.com/v1"
BAD=("facebook.","instagram.","linkedin.","reclameaqui.","econodata.","cnpj.biz","solutudo.","google.","youtube.","jusbrasil.","escavador.")

def pd(path, params=None):
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

def search_site(name):
    q=quote_plus(f'"{name}" site oficial')
    url=f"https://duckduckgo.com/html/?q={q}"
    html=requests.get(url,headers={"User-Agent":"Mozilla/5.0"},timeout=20).text
    links=re.findall(r'href="(https?://[^"]+)"',html)
    for link in links:
        host=urlparse(link).netloc.lower().replace("www.","")
        if host and not any(b in host for b in BAD):
            return host,link
    return "",""

orgs=get_orgs()
rows=[]
for o in orgs:
    name=o.get("name") or ""
    if not name: continue
    site,link=search_site(name)
    rows.append([o.get("id"),name,site,link])
    print(o.get("id"),name,"=>",site)
    time.sleep(1.5)

out="runtime/org_sites_dryrun.csv"
with open(out,"w",newline="",encoding="utf-8") as f:
    w=csv.writer(f); w.writerow(["org_id","empresa","dominio","url"]); w.writerows(rows)

print("OUT=",out,"TOTAL=",len(rows),"COM_SITE=",sum(1 for r in rows if r[2]))
