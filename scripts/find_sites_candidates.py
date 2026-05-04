import os,re,csv,time,requests,html
from urllib.parse import quote_plus, urlparse, parse_qs, unquote

BAD=("duckduckgo","facebook","instagram","linkedin","reclameaqui","econodata","cnpj","jusbrasil","escavador","solutudo","google","youtube","wikipedia")
TOKEN=os.getenv("PD_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"

def pd_orgs():
    out=[]; start=0
    while True:
        r=requests.get(BASE+"/organizations",params={"api_token":TOKEN,"start":start,"limit":500},timeout=30).json()
        out+=r.get("data") or []
        pg=(r.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"): break
        start=pg.get("next_start",0)
    return out

def domain_from_url(u):
    u=html.unescape(u)
    if "uddg=" in u:
        u=unquote(parse_qs(urlparse(u).query).get("uddg",[""])[0])
    host=urlparse(u).netloc.lower().replace("www.","")
    return host

def score(name, dom):
    words=[w.lower() for w in re.findall(r"[a-zA-ZÀ-ÿ0-9]+",name) if len(w)>2]
    d=dom.lower()
    pts=sum(1 for w in words if re.sub(r"[^a-z0-9]","",w)[:6] in d)
    if ".com.br" in d: pts+=1
    return pts

def search(name):
    q=quote_plus(f'{name} site oficial')
    url=f"https://html.duckduckgo.com/html/?q={q}"
    txt=requests.get(url,headers={"User-Agent":"Mozilla/5.0"},timeout=20).text
    urls=re.findall(r'href="([^"]+)"',txt)
    cands=[]
    for u in urls:
        dom=domain_from_url(u)
        if "." not in dom or any(b in dom for b in BAD): continue
        sc=score(name,dom)
        if sc>=1: cands.append((sc,dom,u))
    cands=sorted(set(cands), reverse=True)
    return cands[:3]

rows=[]
for o in pd_orgs():
    name=o.get("name","")
    c=search(name)
    best=c[0] if c else ("","","")
    print(o.get("id"), name, "=>", best[1])
    rows.append([o.get("id"),name,best[1],best[0],best[2],"; ".join(x[1] for x in c),""])
    time.sleep(1)

out="runtime/org_sites_candidates.csv"
with open(out,"w",newline="",encoding="utf-8") as f:
    w=csv.writer(f)
    w.writerow(["org_id","empresa","dominio_sugerido","score","url_fonte","alternativas","status_revisao"])
    w.writerows(rows)
print("OUT=",out)
