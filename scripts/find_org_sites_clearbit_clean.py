import os,re,csv,time,requests

TOKEN=os.getenv("PD_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")
BASE="https://api.pipedrive.com/v1"

REMOVE=r"\b(LTDA|S\.A\.|SA|ME|EPP|EIRELI|COMERCIO|COMÉRCIO|INDUSTRIA|INDÚSTRIA|DISTRIBUICAO|DISTRIBUIÇÃO|SERVICOS|SERVIÇOS|DE|DA|DO|DAS|DOS|E)\b"

def clean(n):
    n=re.sub(r"[^\wÀ-ÿ\s]"," ",str(n),flags=re.I)
    n=re.sub(REMOVE," ",n,flags=re.I)
    n=re.sub(r"\s+"," ",n).strip()
    return n.title()

def orgs():
    out=[]; start=0
    while True:
        r=requests.get(BASE+"/organizations",params={"api_token":TOKEN,"start":start,"limit":500},timeout=30).json()
        out+=r.get("data") or []
        pg=(r.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"): break
        start=pg.get("next_start",0)
    return out

def cb(q):
    try:
        r=requests.get("https://autocomplete.clearbit.com/v1/companies/suggest",params={"query":q},timeout=10).json()
        return (r[0].get("domain",""), r[0].get("name","")) if r else ("","")
    except Exception:
        return "",""

rows=[]
for o in orgs():
    raw=o.get("name","")
    q=clean(raw)
    domain,found=cb(q)
    print(raw,"=>",q,"=>",domain)
    rows.append([o.get("id"),raw,q,domain,found,""])
    time.sleep(.3)

out="runtime/org_sites_clearbit_clean.csv"
with open(out,"w",newline="",encoding="utf-8") as f:
    csv.writer(f).writerows([["org_id","empresa","busca_limpa","dominio","nome_encontrado","status_revisao"],*rows])
print("OUT",out)
