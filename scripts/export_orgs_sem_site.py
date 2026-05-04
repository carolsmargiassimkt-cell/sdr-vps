import os,csv,requests,re

TOKEN=os.getenv("PD_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
assert TOKEN, "export PD_TOKEN=SEU_TOKEN"

BASE="https://api.pipedrive.com/v1"

def get(path, params=None):
    out=[]; start=0
    while True:
        p={**(params or {}),"api_token":TOKEN,"start":start,"limit":500}
        r=requests.get(BASE+path,params=p,timeout=30).json()
        data=r.get("data") or []
        out += data if isinstance(data,list) else [data]
        pg=(r.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"): break
        start=pg.get("next_start",0)
    return out

def clean_name(s):
    s=str(s or "").strip()
    s=re.sub(r"\s+"," ",s)
    return s

orgs=get("/organizations")
rows=[]

for o in orgs:
    org_id=o.get("id")
    name=clean_name(o.get("name"))
    if not name:
        continue

    rows.append([
        org_id,
        name,
        "",  # dominio_encontrado
        "",  # url_fonte
        ""   # status_revisao
    ])

out="runtime/orgs_para_buscar_site_revisao.csv"
with open(out,"w",newline="",encoding="utf-8") as f:
    w=csv.writer(f)
    w.writerow(["org_id","empresa","dominio_encontrado","url_fonte","status_revisao"])
    w.writerows(rows)

print("OUT=",out)
print("TOTAL_ORGS=",len(rows))
